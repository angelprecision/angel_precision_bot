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
DEFAULT_REVALIDATE_TOP_N = int(os.getenv("CONTRACT_REVALIDATE_TOP_N", "5"))

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
    return f"{_broker_cache_identity(broker)}:{occ_symbol}"


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
        "_quote_payload_empty":   not bool(raw),
    }


def fetch_direct_option_quote_with_meta(
    broker,
    occ_symbol: str,
    *,
    cache_ttl_s: Optional[float] = None,
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

    # P0 (PR #296): throttle before direct OCC quote fetch. No-op when
    # TRADIER_MD_THROTTLE_ENABLED=0 (default). Provider errors are NOT
    # suppressed — if broker.get_quote() raises or returns empty/429/zero,
    # the existing reason taxonomy (DIRECT_QUOTE_RATE_LIMITED,
    # DIRECT_QUOTE_ZERO_BID_ASK, etc.) fires unchanged.
    # t0 is set AFTER the throttle sleep so that latency_ms and
    # quote_fetch_latency_ms in orders.meta reflect the actual broker call
    # duration, not the combined throttle-wait + broker-call duration.
    # The cache timestamp (fetched_at) uses the same t0, so the cache entry
    # represents when the broker was actually reached, not when the call was
    # queued.
    try:
        from ap.tradier_market_data_throttle import (
            before_market_data_call,
            after_market_data_call,
        )
        before_market_data_call(
            "/v1/markets/quotes", occ_symbol,
            context="direct_quote_revalidator",
        )
    except Exception:
        pass
    t0 = _now()  # start timing AFTER throttle sleep
    try:
        raw = broker.get_quote(occ_symbol) or {}
    except Exception as e:
        log.warning(
            "fetch_direct_option_quote: broker.get_quote raised contract=%s err=%s",
            occ_symbol, e,
        )
        return _classify_direct_quote_exception(e)
    finally:
        try:
            after_market_data_call()
        except Exception:
            pass

    latency_ms = int((_now() - t0) * 1000)
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

    quote_meta = fetch_direct_option_quote_with_meta(broker, occ)
    if not quote_meta.get("ok"):
        audit = dict(audit_base)
        audit["direct_quote_error"] = quote_meta.get("error")
        audit["direct_quote_endpoint"] = quote_meta.get("endpoint")
        audit["direct_quote_status_code"] = quote_meta.get("status_code")
        audit["direct_quote_retryable"] = quote_meta.get("retryable")
        audit["contract_quote_source"] = "none"
        return {
            "action":            "REJECT_UNAVAILABLE",
            "reason_code":       quote_meta.get("reason_code") or REASON_DIRECT_QUOTE_UNAVAILABLE,
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
