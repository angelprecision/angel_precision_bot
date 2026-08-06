"""
ap/contract_quote_revalidator.py

Purpose
-------
When the option chain (markets/options/chains) returns stale or zero
bid/ask for a candidate contract during market hours, fetch a direct
option quote (markets/quotes) for that exact OCC symbol and re-evaluate.
This restores trade flow for liquid tickers like NVDA, SPY, TSLA, AMD
without weakening any safety rule.

Scope
-----
This module is a pure helper.  It does not touch:
  - scanner scoring
  - signal admission
  - watcher trigger/stop/invalidation
  - exits, broker close, reconciler
  - live capital safety
  - final order submit (P0B is implemented in execution.py)

The contract selector imports `revalidate_with_direct_quote` and calls it
ONLY when a chain-row reject would otherwise fire for one of:
  - zero_bid_or_ask
  - bid_below_0.1
  - NO_CHAIN_DATA
  - missing bid/ask
  - zero liquidity fields

Safety contract
---------------
Direct quote results are accepted ONLY if they pass the same hard rules:
  - bid > 0 AND ask > 0
  - ask >= bid (no inverted books)
  - spread within max_spread_pct
  - premium within configured premium bounds
  - affordable at execution_price (ask for live, mid for paper)

If the direct quote is missing or also returns zero, we reject with
DIRECT_QUOTE_ZERO_BID_ASK or a structured DIRECT_QUOTE_* fetch reason —
never accept a true-zero quote, never bypass spread/premium/capital limits.

PR: hotfix/p0-direct-option-quote-revalidation
"""
from __future__ import annotations

import os
import time
import logging
from datetime import datetime, time as dtime
from typing import Optional

try:
    # Use Eastern time for US market hours.
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover — defensive
    _ET = None

log = logging.getLogger("angel.contract_quote_revalidator")

# ── Configuration ───────────────────────────────────────────────────────────
# Top N candidate option symbols to fetch direct quotes for when chain rows
# look bad.  Keeping this small keeps the Tradier rate-limit budget bounded.
def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        log.warning(
            "DIRECT_QUOTE_ENV_PARSE_ERROR key=%s value=%r expected_type=positive_int default=%s",
            name,
            raw,
            default,
        )
        return default
    if value <= 0:
        log.warning(
            "DIRECT_QUOTE_ENV_PARSE_ERROR key=%s value=%r expected_type=positive_int default=%s",
            name,
            raw,
            default,
        )
        return default
    return value


# Compatibility diagnostic only.  The behavioral request cap is owned by
# SELECTOR_MAX_DIRECT_QUOTE_CALLS in contract_selector.
DEFAULT_REVALIDATE_TOP_N = _positive_int_env("CONTRACT_REVALIDATE_TOP_N", 5)

# Per-transport/per-symbol cache so a single selector pass doesn't double-fetch.
# Cleared per process; tests can reset by calling clear_quote_cache().
_QUOTE_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = float(os.getenv("CONTRACT_REVALIDATE_CACHE_TTL_S", "3.0"))

# ── Reject reason codes (used by selector and tests) ────────────────────────
REASON_CHAIN_ROW_ZERO_BID_ASK         = "CHAIN_ROW_ZERO_BID_ASK"
REASON_DIRECT_QUOTE_ZERO_BID_ASK      = "DIRECT_QUOTE_ZERO_BID_ASK"
REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO = "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO"
REASON_DIRECT_QUOTE_UNAVAILABLE       = "DIRECT_QUOTE_UNAVAILABLE"
REASON_DIRECT_QUOTE_FETCH_TIMEOUT     = "DIRECT_QUOTE_FETCH_TIMEOUT"
REASON_DIRECT_QUOTE_RATE_LIMITED      = "DIRECT_QUOTE_RATE_LIMITED"
REASON_DIRECT_QUOTE_AUTH_FAILED       = "DIRECT_QUOTE_AUTH_FAILED"
REASON_DIRECT_QUOTE_SERVER_ERROR      = "DIRECT_QUOTE_SERVER_ERROR"
REASON_FINAL_CONTRACT_QUOTE_INVALID   = "FINAL_CONTRACT_QUOTE_INVALID"
REASON_FINAL_SPREAD_TOO_WIDE          = "FINAL_SPREAD_TOO_WIDE"
REASON_FINAL_CONTRACT_UNAFFORDABLE    = "FINAL_CONTRACT_UNAFFORDABLE"
REASON_LIQUIDITY_BELOW_THRESHOLD      = "LIQUIDITY_BELOW_THRESHOLD"
REASON_MARKET_DATA_THROTTLE_UNAVAILABLE = "MARKET_DATA_THROTTLE_UNAVAILABLE"
REASON_SELECTOR_REQUEST_BUDGET_EXHAUSTED = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
ACTION_SKIP_BUDGET_EXHAUSTED = "SKIP_BUDGET_EXHAUSTED"

# Reasons that warrant direct quote revalidation.
#
# NOTE: NO_CHAIN_DATA and NO_AFFORDABLE_CONTRACT are intentionally preserved here
# for backward compatibility with the existing selector/tests. RQ4 should narrow
# these in a separate behavior-changing PR because it changes which contracts get
# a direct-quote repair attempt.
_CHAIN_REJECT_REASONS_TO_REVALIDATE = frozenset({
    "zero_bid_or_ask",
    "bid_below_0.1",
    "NO_CHAIN_DATA",
    "NO_AFFORDABLE_CONTRACT",
    "missing_bid_ask",
    "low_volume_0",
    "low_oi_0",
})


def is_market_open(now: Optional[datetime] = None) -> bool:
    """
    Returns True only during US regular hours (9:30–16:00 ET, Mon-Fri).
    Conservative: returns False if zoneinfo isn't available so we never
    revalidate when we can't be sure we're in regular hours.
    """
    if _ET is None:
        return False
    now = now or datetime.now(_ET)
    if now.weekday() >= 5:  # Sat/Sun
        return False
    t = now.time()
    return dtime(9, 30) <= t <= dtime(16, 0)


def should_revalidate(reject_reason: str) -> bool:
    """
    Returns True when the chain-row reject reason warrants a direct quote
    revalidation attempt.
    """
    if not reject_reason:
        return False
    r = str(reject_reason).strip()
    if r in _CHAIN_REJECT_REASONS_TO_REVALIDATE:
        return True
    # Tolerate prefixed variants used elsewhere in selector logs
    for needle in _CHAIN_REJECT_REASONS_TO_REVALIDATE:
        if r.startswith(needle):
            return True
    return False


def clear_quote_cache() -> None:
    """Test helper: reset the per-process direct-quote cache."""
    _QUOTE_CACHE.clear()


def _now() -> float:
    return time.time()


def _broker_cache_identity(broker) -> str:
    """Stable enough identity for direct-quote cache isolation.

    The same OCC symbol may be queried through live-data, paper/sandbox, or stub
    transports inside one process. A symbol-only cache can accidentally share a
    quote across those transports. Use the broker/cfg base_url when available,
    then fall back to explicit source-ish attributes, then class name.
    """
    if broker is None:
        return "none"

    cfg = getattr(broker, "cfg", None)
    for owner in (cfg, broker):
        for attr in ("base_url", "quote_base_url", "data_base_url"):
            value = getattr(owner, attr, None)
            if value:
                return str(value)

    return type(broker).__name__


def _cache_key_for(broker, occ_symbol: str) -> str:
    return f"{_broker_cache_identity(broker)}:{_normalize_occ_symbol(occ_symbol)}"


def _normalize_occ_symbol(occ_symbol: str) -> str:
    """Canonicalize compact and root-padded OCC forms to one request key."""
    return "".join(str(occ_symbol or "").upper().split())


def _ctx_update_sink(request_context) -> None:
    if request_context is None:
        return
    sink = getattr(request_context, "diagnostics_sink", None)
    if not isinstance(sink, dict):
        return
    sink["provider_call_counts"] = dict(getattr(request_context, "provider_call_counts", {}) or {})
    sink["elapsed_ms_by_stage"] = dict(getattr(request_context, "elapsed_ms_by_stage", {}) or {})
    counts = sink["provider_call_counts"]
    used = int(counts.get("direct_quote_calls", 0) or 0)
    # Generic fallback is the ordinary pre-PR #401 default of five. A real
    # deferred request carries its explicit 40 on the context, so this fallback
    # never reduces deferred capacity.
    effective = int(
        getattr(
            request_context,
            "effective_direct_quote_limit",
            getattr(request_context, "max_direct_quote_calls", 5),
        )
        or 5
    )
    remaining = max(0, effective - used)
    sink["direct_quote_calls"] = used
    sink["direct_quote_attempts_remaining"] = remaining
    budget = sink.setdefault("direct_quote_budget", {})
    if isinstance(budget, dict):
        budget.update({
            "canonical_env": getattr(request_context, "configured_selector_max_direct_quote_calls", None),
            "direct_recovery_alias": getattr(request_context, "configured_direct_quote_recovery_top_n", None),
            "contract_revalidate_alias": getattr(request_context, "configured_contract_revalidate_top_n", None),
            "source": getattr(request_context, "direct_quote_budget_source", "default"),
            "effective_limit": effective,
            "used": used,
            "remaining": remaining,
            "conflict": bool(getattr(request_context, "direct_quote_budget_conflict", False)),
            "conflict_detail": getattr(request_context, "direct_quote_budget_conflict_detail", None),
        })
    sink["direct_quote_attempted_symbols"] = list(getattr(request_context, "direct_quote_attempted_symbols", []) or [])
    sink["direct_quote_unattempted_count"] = int(getattr(request_context, "direct_quote_unattempted_count", 0) or 0)
    sink["direct_quote_unattempted_symbols"] = list((getattr(request_context, "direct_quote_unattempted_symbols", []) or [])[:25])
    sink["budget_exhausted_stage"] = getattr(request_context, "budget_exhausted_stage", None)
    sink["budget_exhausted_detail"] = getattr(request_context, "budget_exhausted_detail", None)


def _direct_quote_budget_failure(request_context) -> Optional[dict]:
    if request_context is None:
        return None
    started = float(getattr(request_context, "started_at_monotonic", 0.0) or 0.0)
    elapsed_ms = max(0.0, (time.monotonic() - started) * 1000.0) if started else 0.0
    # Generic fallbacks are the ordinary pre-PR #401 defaults (elapsed 15000ms,
    # five direct quotes). A real deferred request carries its explicit 25000ms
    # / 40-call values on the context, so these fallbacks never reduce deferred
    # capacity.
    elapsed_limit = int(getattr(request_context, "max_total_elapsed_ms", 15000) or 15000)
    counts = getattr(request_context, "provider_call_counts", {}) or {}
    used = int(counts.get("direct_quote_calls", 0) or 0)
    call_limit = int(
        getattr(
            request_context,
            "effective_direct_quote_limit",
            getattr(request_context, "max_direct_quote_calls", 5),
        )
        or 5
    )
    detail = None
    if elapsed_ms >= elapsed_limit:
        detail = f"elapsed_ms={elapsed_ms:.3f} limit_ms={elapsed_limit}"
    elif used >= call_limit:
        detail = f"direct_quote_calls={used} limit={call_limit}"
    if detail is None:
        return None
    setattr(request_context, "budget_exhausted_stage", "direct_quote")
    setattr(request_context, "budget_exhausted_detail", detail)
    _ctx_update_sink(request_context)
    return {
        "ok": False,
        "quote": None,
        "reason_code": REASON_SELECTOR_REQUEST_BUDGET_EXHAUSTED,
        "error": detail,
        "endpoint": "/v1/markets/quotes",
        "status_code": None,
        "retryable": False,
    }


def _ctx_increment_call_count(request_context, key: str, count: int = 1) -> None:
    if request_context is None:
        return
    counts = getattr(request_context, "provider_call_counts", None)
    if isinstance(counts, dict):
        counts[key] = int(counts.get(key, 0) or 0) + count
        _ctx_update_sink(request_context)


def _ctx_note_attempted_symbol(request_context, occ_symbol: str) -> None:
    if request_context is None:
        return
    attempted = getattr(request_context, "direct_quote_attempted_symbols", None)
    if isinstance(attempted, list) and occ_symbol not in attempted:
        attempted.append(occ_symbol)
        _ctx_update_sink(request_context)


def _ctx_persist_attempt(
    request_context,
    occ_symbol: str,
    *,
    result_reason: str,
    transient: bool,
    provider_timestamp=None,
) -> None:
    """Persist completed quote progress through the existing order-state owner."""
    if request_context is None:
        return
    callback = getattr(request_context, "recovery_cursor_persist", None)
    if not callable(callback):
        return
    try:
        callback(
            symbol=str(occ_symbol or ""),
            result_reason=str(result_reason or ""),
            transient=bool(transient),
            provider_timestamp=provider_timestamp,
        )
    except Exception as exc:
        from ap.selector_retry_policy import (
            SelectorRecoveryCursorPersistFailed,
            SelectorRecoveryOwnershipLost,
        )
        if isinstance(exc, SelectorRecoveryOwnershipLost):
            raise
        # Audit follow-up fix: previously logged and continued, letting
        # the selector keep spending provider-call/broker-adjacent budget
        # on later candidates with no durable record of this candidate's
        # outcome -- violating the durable restart contract exactly like
        # batching did (already fixed separately), just via write-failure
        # rather than write-delay. Any non-ownership persistence failure
        # (DB exception, serialization error, timeout, or anything else)
        # now stops selector work immediately with a stable typed failure
        # rather than continuing on process-memory-only progress.
        log.warning(
            "selector recovery cursor persist failed contract=%s reason=%s err=%s",
            occ_symbol,
            result_reason,
            exc,
        )
        raise SelectorRecoveryCursorPersistFailed(
            f"cursor persist failed for {occ_symbol}: {exc}"
        ) from exc


def correct_recovered_cursor_disposition(
    request_context,
    occ_symbol: str,
    *,
    final_reason: str,
    provider_timestamp=None,
) -> None:
    """Audit blocker 4 fix: correct a durable cursor record that was
    written prematurely as DIRECT_QUOTE_RECOVERED_CHAIN_ZERO/transient=False
    at the transport-recovery stage, before the caller's own subsequent
    spread/OI/volume (_quality_filter) and delta/premium/affordability
    checks completed. This function's own docstring at the original write
    site says outright: "Caller will re-run the spread/premium/
    affordability/liquidity checks against this patched opt" -- meaning the
    original write is known-provisional at the moment it happens.

    Call this once the caller's full quality re-run has actually rejected
    the candidate, with the real final reason code (e.g. "SPREAD_TOO_WIDE",
    "OI_TOO_LOW"), so the durable cursor never remains classified as a
    successful recovered candidate once it's known not to be one. Uses the
    same fail-closed persistence path (_ctx_persist_attempt) as the
    original write -- a failure to persist the correction raises
    SelectorRecoveryCursorPersistFailed exactly like any other cursor
    write, rather than silently leaving the incorrect record in place.
    """
    _ctx_persist_attempt(
        request_context,
        occ_symbol,
        result_reason=str(final_reason or "UNKNOWN_FAIL_CLOSED"),
        transient=False,
        provider_timestamp=provider_timestamp,
    )


def _ctx_persist_structural_skip(
    request_context,
    occ_symbol: str,
    *,
    structural_skip_reason: str,
) -> None:
    """Persist a structural-skip event through the same fail-closed
    contract as _ctx_persist_attempt(). Previously duplicated inline in
    ap/contract_selector.py's _structural_direct_quote_skip() with its own
    try/except that only re-raised SelectorRecoveryOwnershipLost and
    logged-and-continued for every other exception -- including the typed
    SelectorRecoveryCursorPersistFailed exception this function itself now
    raises, meaning a structural skip could be recorded only in process
    memory while the selector kept spending provider-call budget on later
    candidates. Factored into this one shared function rather than fixed
    twice in two places that could drift apart again.
    """
    if request_context is None:
        return
    callback = getattr(request_context, "recovery_cursor_persist", None)
    if not callable(callback):
        return
    try:
        callback(
            symbol=str(occ_symbol or ""),
            structural_skip_reason=str(structural_skip_reason or ""),
        )
    except Exception as exc:
        from ap.selector_retry_policy import (
            SelectorRecoveryCursorPersistFailed,
            SelectorRecoveryOwnershipLost,
        )
        if isinstance(exc, SelectorRecoveryOwnershipLost):
            raise
        log.warning(
            "selector recovery structural cursor persist failed symbol=%s err=%s",
            occ_symbol,
            exc,
        )
        raise SelectorRecoveryCursorPersistFailed(
            f"structural skip persist failed for {occ_symbol}: {exc}"
        ) from exc


def _ctx_note_unattempted_symbol(request_context, occ_symbol: str) -> None:
    if request_context is None:
        return
    seen = getattr(request_context, "direct_quote_unattempted_set", None)
    if isinstance(seen, set):
        if occ_symbol in seen:
            _ctx_update_sink(request_context)
            return
        seen.add(occ_symbol)
    symbols = getattr(request_context, "direct_quote_unattempted_symbols", None)
    if isinstance(symbols, list) and occ_symbol not in symbols and len(symbols) < 25:
        symbols.append(occ_symbol)
    current = len(seen) if isinstance(seen, set) else len(symbols or [])
    setattr(request_context, "direct_quote_unattempted_count", current)
    _ctx_update_sink(request_context)


def _ctx_add_stage_ms(request_context, key: str, elapsed_ms: float) -> None:
    if request_context is None:
        return
    buckets = getattr(request_context, "elapsed_ms_by_stage", None)
    if isinstance(buckets, dict):
        buckets[key] = float(buckets.get(key, 0.0) or 0.0) + float(elapsed_ms)
        _ctx_update_sink(request_context)


def _ctx_add_throttle_wait(request_context, wait_ms: float) -> None:
    if request_context is None:
        return
    current = float(getattr(request_context, "throttle_wait_ms", 0.0) or 0.0)
    setattr(request_context, "throttle_wait_ms", current + float(wait_ms or 0.0))
    _ctx_update_sink(request_context)


def _ctx_note_throttle_issue(request_context, *, endpoint: str, symbol: str, context: str, phase: str, error: Exception) -> None:
    if request_context is None:
        return
    diags = getattr(request_context, "throttle_diagnostics", None)
    if isinstance(diags, list):
        diags.append({
            "endpoint": str(endpoint),
            "symbol": str(symbol),
            "context": str(context),
            "phase": str(phase),
            "error": str(error),
        })
        _ctx_update_sink(request_context)


def _exception_status_code(exc: Exception) -> Optional[int]:
    status = getattr(exc, "status_code", None)
    if status is None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _classify_direct_quote_exception(exc: Exception) -> dict:
    """Map broker/HTTP exceptions into stable, queryable direct-quote reasons."""
    endpoint = getattr(exc, "endpoint", None) or "/v1/markets/quotes"
    status_code = _exception_status_code(exc)
    reason_code = getattr(exc, "reason_code", None)
    retryable = getattr(exc, "retryable", None)

    if not reason_code:
        name = type(exc).__name__.lower()
        msg = str(exc).lower()

        if "timeout" in name or "timeout" in msg or "timed out" in msg:
            reason_code = REASON_DIRECT_QUOTE_FETCH_TIMEOUT
        elif status_code in (401, 403):
            reason_code = REASON_DIRECT_QUOTE_AUTH_FAILED
        elif status_code == 429:
            reason_code = REASON_DIRECT_QUOTE_RATE_LIMITED
        elif status_code is not None and 500 <= status_code <= 599:
            reason_code = REASON_DIRECT_QUOTE_SERVER_ERROR
        else:
            reason_code = REASON_DIRECT_QUOTE_UNAVAILABLE

    if retryable is None:
        retryable = reason_code in {
            REASON_DIRECT_QUOTE_FETCH_TIMEOUT,
            REASON_DIRECT_QUOTE_RATE_LIMITED,
            REASON_DIRECT_QUOTE_SERVER_ERROR,
        }

    return {
        "ok": False,
        "quote": None,
        "reason_code": str(reason_code),
        "error": str(exc),
        "endpoint": str(endpoint),
        "status_code": status_code,
        "retryable": bool(retryable),
    }


def _empty_quote_failure() -> dict:
    return {
        "ok": False,
        "quote": None,
        "reason_code": REASON_DIRECT_QUOTE_UNAVAILABLE,
        "error": "empty quote payload",
        "endpoint": "/v1/markets/quotes",
        "status_code": None,
        "retryable": None,
    }


def _normalize_quote(raw: dict, fetched_at: float, latency_ms: int) -> dict:
    def _f(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _i(v):
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    bid = _f(raw.get("bid"))
    ask = _f(raw.get("ask"))
    last = _f(raw.get("last"))

    return {
        "bid":                  bid,
        "ask":                  ask,
        "last":                 last,
        "bid_size":             _i(raw.get("bidsize") or raw.get("bid_size")),
        "ask_size":             _i(raw.get("asksize") or raw.get("ask_size")),
        "volume":               _i(raw.get("volume")),
        "open_interest":        _i(raw.get("open_interest")),
        # Backward-compatible name. This is not exchange quote age.
        "quote_age_ms":         latency_ms,
        "fetched_at":           fetched_at,
        # Explicit names for the true semantics.
        "quote_fetch_latency_ms": latency_ms,
        "quote_fetched_at":       fetched_at,
        "quote_age_semantics":    "fetch_latency_not_exchange_age",
        "provider_timestamp":     (
            raw.get("provider_timestamp")
            or raw.get("quote_timestamp")
            or raw.get("timestamp")
        ),
        "_quote_payload_empty":   not bool(raw),
    }


def fetch_direct_option_quote_with_meta(
    broker,
    occ_symbol: str,
    *,
    cache_ttl_s: Optional[float] = None,
    request_context=None,
) -> dict:
    """
    Fetch a direct option quote and preserve structured broker failure metadata.

    Returns:
      {
        "ok": bool,
        "quote": dict | None,
        "reason_code": str | None,
        "error": str | None,
        "endpoint": str,
        "status_code": int | None,
        "retryable": bool | None,
      }

    Unlike fetch_direct_option_quote(), empty broker payloads are treated as a
    structured DIRECT_QUOTE_UNAVAILABLE for callers making a decision.
    """
    ttl = cache_ttl_s if cache_ttl_s is not None else _CACHE_TTL_S
    if not occ_symbol or broker is None:
        return _empty_quote_failure()
    occ_symbol = _normalize_occ_symbol(occ_symbol)

    cache_key = _cache_key_for(broker, occ_symbol)
    cached = _QUOTE_CACHE.get(cache_key)
    if cached and (_now() - cached[0]) < ttl:
        quote = cached[1]
        if quote.get("_quote_payload_empty"):
            return _empty_quote_failure()
        return {
            "ok": True,
            "quote": quote,
            "reason_code": None,
            "error": None,
            "endpoint": "/v1/markets/quotes",
            "status_code": None,
            "retryable": None,
        }

    budget_failure = _direct_quote_budget_failure(request_context)
    if budget_failure is not None:
        return budget_failure

    def _throttle_failure(exc: Exception, phase: str) -> Optional[dict]:
        mode = str(getattr(request_context, "execution_mode", "unknown") or "unknown").lower()
        paper_unthrottled = (
            mode == "paper"
            and os.getenv(
                "SELECTOR_PAPER_ALLOW_UNTHROTTLED_DIRECT_QUOTES", "0"
            ).strip().lower() in ("1", "true", "yes")
        )
        diagnostics = getattr(request_context, "throttle_diagnostics", None)
        if isinstance(diagnostics, list) and diagnostics:
            diagnostics[-1]["paper_unthrottled_override"] = paper_unthrottled
            diagnostics[-1]["provider_call_blocked"] = not paper_unthrottled
            _ctx_update_sink(request_context)
        if paper_unthrottled:
            log.warning(
                "direct quote proceeding unthrottled by explicit PAPER policy "
                "contract=%s phase=%s",
                occ_symbol, phase,
            )
            return None
        return {
            "ok": False,
            "quote": None,
            "reason_code": REASON_MARKET_DATA_THROTTLE_UNAVAILABLE,
            "error": f"throttle {phase} failed: {exc}",
            "endpoint": "/v1/markets/quotes",
            "status_code": None,
            "retryable": False,
        }

    # P0 (PR #296): throttle before direct OCC quote fetch. No-op when
    # TRADIER_MD_THROTTLE_ENABLED=0 (default). Provider errors are NOT
    # suppressed — if broker.get_quote() raises or returns empty/429/zero,
    # the existing reason taxonomy (DIRECT_QUOTE_RATE_LIMITED,
    # DIRECT_QUOTE_ZERO_BID_ASK, etc.) fires unchanged.
    # t0 is intentionally set AFTER the throttle returns so that
    # quote_fetch_latency_ms in orders.meta reflects the actual broker
    # round-trip, not the combined throttle-wait + broker-call duration.
    throttle_token = None
    try:
        from ap.tradier_market_data_throttle import (
            before_market_data_call,
            after_market_data_call,
        )
    except Exception as exc:
        log.error(
            "fetch_direct_option_quote throttle import failed contract=%s err=%s",
            occ_symbol, exc,
        )
        _ctx_note_throttle_issue(
            request_context,
            endpoint="/v1/markets/quotes",
            symbol=occ_symbol,
            context="direct_quote_revalidator",
            phase="import",
            error=exc,
        )
        failure = _throttle_failure(exc, "import")
        if failure is not None:
            return failure
        after_market_data_call = None
    else:
        try:
            throttle_token = before_market_data_call(
                "/v1/markets/quotes", occ_symbol,
                context="direct_quote_revalidator",
            )
            _ctx_add_throttle_wait(request_context, (throttle_token or {}).get("wait_ms", 0.0))
        except Exception as exc:
            log.error(
                "fetch_direct_option_quote throttle acquire failed contract=%s err=%s",
                occ_symbol, exc,
            )
            _ctx_note_throttle_issue(
                request_context,
                endpoint="/v1/markets/quotes",
                symbol=occ_symbol,
                context="direct_quote_revalidator",
                phase="acquire",
                error=exc,
            )
            failure = _throttle_failure(exc, "acquire")
            if failure is not None:
                return failure
            throttle_token = None
    t0 = _now()  # start timing AFTER throttle sleep — latency reflects broker only
    _ctx_note_attempted_symbol(request_context, occ_symbol)
    _ctx_increment_call_count(request_context, "direct_quote_calls")
    try:
        raw = broker.get_quote(occ_symbol) or {}
    except Exception as e:
        log.warning(
            "fetch_direct_option_quote: broker.get_quote raised contract=%s err=%s",
            occ_symbol, e,
        )
        return _classify_direct_quote_exception(e)
    finally:
        if throttle_token is not None and after_market_data_call is not None:
            try:
                after_market_data_call()
            except Exception:
                pass

    latency_ms = int((_now() - t0) * 1000)
    _ctx_add_stage_ms(request_context, "direct_quote", latency_ms)
    quote = _normalize_quote(raw, t0, latency_ms)
    _QUOTE_CACHE[cache_key] = (t0, quote)

    if quote.get("_quote_payload_empty"):
        return _empty_quote_failure()

    return {
        "ok": True,
        "quote": quote,
        "reason_code": None,
        "error": None,
        "endpoint": "/v1/markets/quotes",
        "status_code": None,
        "retryable": None,
    }


def fetch_direct_option_quote(
    broker,
    occ_symbol: str,
    *,
    cache_ttl_s: Optional[float] = None,
) -> Optional[dict]:
    """
    Fetch a direct option quote for an OCC symbol via broker.get_quote().
    Cached per-process for cache_ttl_s seconds.  Returns:
      {
        "bid": float|None, "ask": float|None, "last": float|None,
        "bid_size": int|None, "ask_size": int|None,
        "volume": int|None, "open_interest": int|None,
        "quote_age_ms": int, "fetched_at": float,
        "quote_fetch_latency_ms": int, "quote_fetched_at": float,
        "quote_age_semantics": "fetch_latency_not_exchange_age",
      }
    Returns None on broker error. Empty broker payloads retain backward-compatible
    behavior and return a normalized quote with None bid/ask.
    """
    ttl = cache_ttl_s if cache_ttl_s is not None else _CACHE_TTL_S
    if not occ_symbol or broker is None:
        return None

    cache_key = _cache_key_for(broker, occ_symbol)
    cached = _QUOTE_CACHE.get(cache_key)
    if cached and (_now() - cached[0]) < ttl:
        return cached[1]

    t0 = _now()
    try:
        raw = broker.get_quote(occ_symbol) or {}
    except Exception as e:
        log.warning(
            "fetch_direct_option_quote: broker.get_quote raised contract=%s err=%s",
            occ_symbol, e,
        )
        return None

    latency_ms = int((_now() - t0) * 1000)
    out = _normalize_quote(raw, t0, latency_ms)
    _QUOTE_CACHE[cache_key] = (t0, out)
    return out


def direct_quote_is_valid(quote: Optional[dict]) -> bool:
    """
    Hard validity check for a direct quote: bid > 0 AND ask > 0 AND ask >= bid.
    """
    if not quote:
        return False
    bid = quote.get("bid")
    ask = quote.get("ask")
    if bid is None or ask is None:
        return False
    try:
        if float(bid) <= 0 or float(ask) <= 0:
            return False
        if float(ask) < float(bid):
            return False
    except (TypeError, ValueError):
        return False
    return True


def revalidate_with_direct_quote(
    broker,
    opt: dict,
    chain_reject_reason: str,
    *,
    market_open_override: Optional[bool] = None,
    request_context=None,
) -> dict:
    """
    Attempt to recover a contract that was about to be rejected from chain
    data alone.  Returns a result dict:

      {
        "action":        "PASS" | "REJECT_DIRECT_ZERO" | "REJECT_UNAVAILABLE"
                         | "SKIP_NOT_MARKET_HOURS" | "SKIP_NOT_REVALIDATABLE",
        "reason_code":   str | None,
        "direct_quote_used": bool,
        "opt_updated":   dict | None,    # opt with bid/ask/last patched, or None
        "audit": {
          "chain_bid":   float|None,
          "chain_ask":   float|None,
          "direct_bid":  float|None,
          "direct_ask":  float|None,
          "direct_mid":  float|None,
          "direct_quote_age_ms": int|None,
          "direct_quote_fetch_latency_ms": int|None,
          "direct_quote_fetched_at": float|None,
          "direct_quote_age_semantics": str|None,
          "contract_quote_source": "chain"|"direct"|"none",
        }
      }

    Action semantics:
      PASS                     → continue to spread/premium/affordability checks
      REJECT_DIRECT_ZERO       → reject; direct quote also zero/invalid
      REJECT_UNAVAILABLE       → reject; direct quote could not be fetched
      SKIP_NOT_MARKET_HOURS    → fall back to original chain reject (off hours)
      SKIP_NOT_REVALIDATABLE   → reject reason is not in our recovery set
    """
    chain_bid = opt.get("bid")
    chain_ask = opt.get("ask")
    audit_base = {
        "chain_bid":                       chain_bid,
        "chain_ask":                       chain_ask,
        "direct_bid":                      None,
        "direct_ask":                      None,
        "direct_mid":                      None,
        "direct_quote_age_ms":             None,
        "direct_quote_fetch_latency_ms":   None,
        "direct_quote_fetched_at":         None,
        "direct_quote_age_semantics":      None,
        "direct_quote_error":              None,
        "direct_quote_endpoint":           None,
        "direct_quote_status_code":        None,
        "direct_quote_retryable":          None,
        "contract_quote_source":           "chain",
    }

    if not should_revalidate(chain_reject_reason):
        return {
            "action":            "SKIP_NOT_REVALIDATABLE",
            "reason_code":       None,
            "direct_quote_used": False,
            "opt_updated":       None,
            "audit":             audit_base,
        }

    market_open = market_open_override if market_open_override is not None else is_market_open()
    if not market_open:
        return {
            "action":            "SKIP_NOT_MARKET_HOURS",
            "reason_code":       None,
            "direct_quote_used": False,
            "opt_updated":       None,
            "audit":             audit_base,
        }

    occ = opt.get("symbol") or opt.get("contract") or ""
    if not occ:
        return {
            "action":            "REJECT_UNAVAILABLE",
            "reason_code":       REASON_DIRECT_QUOTE_UNAVAILABLE,
            "direct_quote_used": False,
            "opt_updated":       None,
            "audit":             audit_base,
        }

    occ_key = _normalize_occ_symbol(occ)
    if request_context is not None:
        revalidated_contracts = getattr(request_context, "revalidated_contracts", None)
        if isinstance(revalidated_contracts, set):
            if occ_key in revalidated_contracts:
                return {
                    "action":            "SKIP_ALREADY_REVALIDATED",
                    "reason_code":       None,
                    "direct_quote_used": False,
                    "opt_updated":       None,
                    "audit":             audit_base,
                }
        budget_failure = _direct_quote_budget_failure(request_context)
        if budget_failure is not None:
            _ctx_note_unattempted_symbol(request_context, occ_key)
            audit = dict(audit_base)
            audit["direct_quote_error"] = budget_failure["error"]
            audit["direct_quote_endpoint"] = budget_failure["endpoint"]
            audit["contract_quote_source"] = "none"
            audit["budget_exhausted"] = True
            audit["original_chain_reject_reason"] = chain_reject_reason
            return {
                "action":            ACTION_SKIP_BUDGET_EXHAUSTED,
                "reason_code":       REASON_SELECTOR_REQUEST_BUDGET_EXHAUSTED,
                "direct_quote_used": False,
                "opt_updated":       None,
                "audit":             audit,
            }
        if isinstance(revalidated_contracts, set):
            revalidated_contracts.add(occ_key)

    quote_meta = fetch_direct_option_quote_with_meta(
        broker,
        occ_key,
        request_context=request_context,
    )
    if not quote_meta.get("ok"):
        audit = dict(audit_base)
        audit["direct_quote_error"] = quote_meta.get("error")
        audit["direct_quote_endpoint"] = quote_meta.get("endpoint")
        audit["direct_quote_status_code"] = quote_meta.get("status_code")
        audit["direct_quote_retryable"] = quote_meta.get("retryable")
        audit["contract_quote_source"] = "none"
        if quote_meta.get("reason_code") == REASON_SELECTOR_REQUEST_BUDGET_EXHAUSTED:
            _ctx_note_unattempted_symbol(request_context, occ_key)
            audit["budget_exhausted"] = True
            audit["original_chain_reject_reason"] = chain_reject_reason
            return {
                "action":            ACTION_SKIP_BUDGET_EXHAUSTED,
                "reason_code":       REASON_SELECTOR_REQUEST_BUDGET_EXHAUSTED,
                "direct_quote_used": False,
                "opt_updated":       None,
                "audit":             audit,
            }
        _failure_reason = quote_meta.get("reason_code") or REASON_DIRECT_QUOTE_UNAVAILABLE
        _ctx_persist_attempt(
            request_context,
            occ_key,
            result_reason=_failure_reason,
            transient=bool(
                quote_meta.get("retryable")
                or _failure_reason
                in {
                    REASON_DIRECT_QUOTE_UNAVAILABLE,
                    REASON_DIRECT_QUOTE_FETCH_TIMEOUT,
                    REASON_DIRECT_QUOTE_RATE_LIMITED,
                    REASON_MARKET_DATA_THROTTLE_UNAVAILABLE,
                }
            ),
        )
        return {
            "action":            "REJECT_UNAVAILABLE",
            "reason_code":       _failure_reason,
            "direct_quote_used": False,
            "opt_updated":       None,
            "audit":             audit,
        }

    quote = quote_meta.get("quote")
    audit = dict(audit_base)
    audit["direct_bid"]                    = quote.get("bid")
    audit["direct_ask"]                    = quote.get("ask")
    audit["direct_quote_age_ms"]           = quote.get("quote_age_ms")
    audit["direct_quote_fetch_latency_ms"] = quote.get("quote_fetch_latency_ms")
    audit["direct_quote_fetched_at"]       = quote.get("quote_fetched_at")
    audit["direct_quote_age_semantics"]    = quote.get("quote_age_semantics")
    if quote.get("bid") is not None and quote.get("ask") is not None:
        try:
            audit["direct_mid"] = (float(quote["bid"]) + float(quote["ask"])) / 2.0
        except (TypeError, ValueError):
            audit["direct_mid"] = None

    if not direct_quote_is_valid(quote):
        _ctx_persist_attempt(
            request_context,
            occ_key,
            result_reason=REASON_DIRECT_QUOTE_ZERO_BID_ASK,
            transient=True,
            provider_timestamp=quote.get("provider_timestamp"),
        )
        return {
            "action":            "REJECT_DIRECT_ZERO",
            "reason_code":       REASON_DIRECT_QUOTE_ZERO_BID_ASK,
            "direct_quote_used": True,
            "opt_updated":       None,
            "audit":             audit,
        }

    # Patch the opt dict with direct-quote values.  Caller will re-run the
    # spread/premium/affordability/liquidity checks against this patched opt.
    patched = dict(opt)
    patched["bid"]  = quote["bid"]
    patched["ask"]  = quote["ask"]
    if quote.get("last") is not None:
        patched["last"] = quote["last"]
    if quote.get("volume") is not None and (patched.get("volume") in (None, 0)):
        patched["volume"] = quote["volume"]
    if quote.get("open_interest") is not None and (patched.get("open_interest") in (None, 0)):
        patched["open_interest"] = quote["open_interest"]
    patched["_direct_quote_used"] = True
    patched["_direct_quote_age_ms"] = quote.get("quote_age_ms")
    patched["_direct_quote_fetch_latency_ms"] = quote.get("quote_fetch_latency_ms")
    patched["_direct_quote_fetched_at"] = quote.get("quote_fetched_at")
    patched["_direct_quote_age_semantics"] = quote.get("quote_age_semantics")
    patched["_chain_bid"] = chain_bid
    patched["_chain_ask"] = chain_ask

    audit["contract_quote_source"] = "direct"
    # Audit Blocker 2, true two-stage fix: previously persisted
    # DIRECT_QUOTE_RECOVERED_CHAIN_ZERO/transient=False here, immediately
    # after a valid transport-level quote and before ANY of the caller's
    # spread/OI/volume/delta/premium/affordability checks ran -- a crash
    # in that window left a false "successful recovery" record on
    # restart, and the later correction (if the checks rejected the
    # candidate) did not close that window, only shortened the time a
    # false record could be read as final. No cursor write happens here
    # at all now. The caller persists the true final disposition exactly
    # once: at its own rejection point if any check fails, or at the one
    # place in the whole selection loop where a candidate has passed
    # every gate and is actually chosen.
    return {
        "action":            "PASS",
        "reason_code":       REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO,
        "direct_quote_used": True,
        "opt_updated":       patched,
        "audit":             audit,
    }


# ============================================================================
# P0B — Final pre-submit direct quote refresh
# ============================================================================

def final_quote_check_before_submit(
    broker,
    contract: str,
    *,
    max_spread_pct: float,
    min_premium: float,
    max_premium: float,
    budget_usd: float,
    is_live: bool,
    qty: int = 1,
) -> dict:
    """
    Final pre-submit direct quote refresh + hard validation gate.

    Called immediately before the broker submit.  Returns:
      {
        "ok": bool,
        "reason_code": str | None,
        "explanation": str,
        "final_bid":  float | None,
        "final_ask":  float | None,
        "final_mid":  float | None,
        "final_last": float | None,
        "spread_pct": float | None,
        "quote_age_ms": int | None,
        "quote_fetch_latency_ms": int | None,
        "quote_fetched_at": float | None,
        "quote_age_semantics": str | None,
        "pricing_basis": "ASK_EXECUTION" | "MID_SIMULATION",
        "execution_price": float | None,
        "execution_cost": float | None,
        "qty": int,
      }

    Hard rejects on:
      - missing/invalid quote                → FINAL_CONTRACT_QUOTE_INVALID
      - bid <= 0 or ask <= 0 or ask < bid    → FINAL_CONTRACT_QUOTE_INVALID
      - spread_pct > max_spread_pct          → FINAL_SPREAD_TOO_WIDE
      - execution-basis premium < min        → FINAL_CONTRACT_QUOTE_INVALID
      - execution-basis premium > max        → FINAL_CONTRACT_UNAFFORDABLE
      - execution_price * 100 * qty > budget → FINAL_CONTRACT_UNAFFORDABLE

    Never bypasses any of these for any reason.
    """
    try:
        _qty = max(1, int(qty))
    except (TypeError, ValueError):
        _qty = 1

    pricing_basis = "ASK_EXECUTION" if is_live else "MID_SIMULATION"

    _null_qf = {
        "final_bid":   None,
        "final_ask":   None,
        "final_mid":   None,
        "final_last":  None,
        "spread_pct":  None,
        "quote_age_ms": None,
        "quote_fetch_latency_ms": None,
        "quote_fetched_at": None,
        "quote_age_semantics": "fetch_latency_not_exchange_age",
        "pricing_basis": pricing_basis,
        "execution_price": None,
        "execution_cost": None,
        "qty": _qty,
    }

    quote_meta = fetch_direct_option_quote_with_meta(broker, contract, cache_ttl_s=0.0)
    if not quote_meta.get("ok"):
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_QUOTE_INVALID,
            "explanation": "final pre-submit quote unavailable",
            "quote_fetch_reason_code": quote_meta.get("reason_code") or REASON_DIRECT_QUOTE_UNAVAILABLE,
            "quote_fetch_error": quote_meta.get("error"),
            "quote_fetch_endpoint": quote_meta.get("endpoint"),
            "quote_fetch_status_code": quote_meta.get("status_code"),
            "quote_fetch_retryable": quote_meta.get("retryable"),
            **_null_qf,
        }

    quote = quote_meta.get("quote")
    bid = quote.get("bid")
    ask = quote.get("ask")
    last = quote.get("last")

    if not direct_quote_is_valid(quote):
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_QUOTE_INVALID,
            "explanation": f"final quote invalid bid={bid} ask={ask}",
            "final_bid":   bid,
            "final_ask":   ask,
            "final_mid":   None,
            "final_last":  last,
            "spread_pct":  None,
            "quote_age_ms": quote.get("quote_age_ms"),
            "quote_fetch_latency_ms": quote.get("quote_fetch_latency_ms"),
            "quote_fetched_at": quote.get("quote_fetched_at"),
            "quote_age_semantics": quote.get("quote_age_semantics"),
            "pricing_basis": pricing_basis,
            "execution_price": None,
            "execution_cost": None,
            "qty": _qty,
        }

    bid_f, ask_f = float(bid), float(ask)
    mid = (bid_f + ask_f) / 2.0
    spread_pct = (ask_f - bid_f) / mid if mid > 0 else None
    execution_price = ask_f if is_live else mid
    execution_cost = execution_price * 100.0 * _qty
    premium_per_contract = execution_price * 100.0

    qf = {
        "final_bid":    bid_f,
        "final_ask":    ask_f,
        "final_mid":    mid,
        "final_last":   last,
        "spread_pct":   spread_pct,
        "quote_age_ms": quote.get("quote_age_ms"),
        "quote_fetch_latency_ms": quote.get("quote_fetch_latency_ms"),
        "quote_fetched_at": quote.get("quote_fetched_at"),
        "quote_age_semantics": quote.get("quote_age_semantics"),
        "pricing_basis": pricing_basis,
        "execution_price": execution_price,
        "execution_cost": execution_cost,
        "qty": _qty,
    }

    if spread_pct is not None and spread_pct > max_spread_pct:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_SPREAD_TOO_WIDE,
            "explanation": f"spread {spread_pct*100:.1f}% > max {max_spread_pct*100:.1f}%",
            **qf,
        }

    if premium_per_contract < min_premium:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_QUOTE_INVALID,
            "explanation": f"premium ${premium_per_contract:.2f} < min ${min_premium:.2f}",
            **qf,
        }
    if premium_per_contract > max_premium:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_UNAFFORDABLE,
            "explanation": f"premium ${premium_per_contract:.2f} > max ${max_premium:.2f}",
            **qf,
        }

    # Validate full order cost (qty contracts) against budget.
    # execution_cost = execution_price * 100 * qty
    if execution_cost > budget_usd:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_UNAFFORDABLE,
            "explanation": (
                f"order cost ${execution_cost:.2f} "
                f"({_qty}x${execution_price:.4f}x100) > budget ${budget_usd:.2f}"
            ),
            **qf,
        }

    return {
        "ok":          True,
        "reason_code": None,
        "explanation": "final quote valid",
        **qf,
    }
