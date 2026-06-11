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
  - mid >= min_premium / 100 and <= max_premium / 100
  - affordable at execution_price (ask for live, mid for paper)

If the direct quote is missing or also returns zero, we reject with
DIRECT_QUOTE_ZERO_BID_ASK or DIRECT_QUOTE_UNAVAILABLE — never accept a
true-zero quote, never bypass spread/premium/capital limits.

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

# Per-symbol cache so a single selector pass doesn't double-fetch.
# Cleared per process; tests can reset by calling clear_quote_cache().
_QUOTE_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL_S = float(os.getenv("CONTRACT_REVALIDATE_CACHE_TTL_S", "3.0"))

# ── Reject reason codes (used by selector and tests) ────────────────────────
REASON_CHAIN_ROW_ZERO_BID_ASK         = "CHAIN_ROW_ZERO_BID_ASK"
REASON_DIRECT_QUOTE_ZERO_BID_ASK      = "DIRECT_QUOTE_ZERO_BID_ASK"
REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO = "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO"
REASON_DIRECT_QUOTE_UNAVAILABLE       = "DIRECT_QUOTE_UNAVAILABLE"
REASON_FINAL_CONTRACT_QUOTE_INVALID   = "FINAL_CONTRACT_QUOTE_INVALID"
REASON_FINAL_SPREAD_TOO_WIDE          = "FINAL_SPREAD_TOO_WIDE"
REASON_FINAL_CONTRACT_UNAFFORDABLE    = "FINAL_CONTRACT_UNAFFORDABLE"
REASON_LIQUIDITY_BELOW_THRESHOLD      = "LIQUIDITY_BELOW_THRESHOLD"

# Reasons that warrant direct quote revalidation
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
      }
    Returns None on broker error or empty result.
    """
    ttl = cache_ttl_s if cache_ttl_s is not None else _CACHE_TTL_S
    if not occ_symbol or broker is None:
        return None

    cached = _QUOTE_CACHE.get(occ_symbol)
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

    out = {
        "bid":           bid,
        "ask":           ask,
        "last":          last,
        "bid_size":      _i(raw.get("bidsize") or raw.get("bid_size")),
        "ask_size":      _i(raw.get("asksize") or raw.get("ask_size")),
        "volume":        _i(raw.get("volume")),
        "open_interest": _i(raw.get("open_interest")),
        "quote_age_ms":  int((_now() - t0) * 1000),
        "fetched_at":    t0,
    }
    _QUOTE_CACHE[occ_symbol] = (t0, out)
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
        "chain_bid":             chain_bid,
        "chain_ask":             chain_ask,
        "direct_bid":            None,
        "direct_ask":            None,
        "direct_mid":            None,
        "direct_quote_age_ms":   None,
        "contract_quote_source": "chain",
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

    quote = fetch_direct_option_quote(broker, occ)
    if quote is None:
        return {
            "action":            "REJECT_UNAVAILABLE",
            "reason_code":       REASON_DIRECT_QUOTE_UNAVAILABLE,
            "direct_quote_used": False,
            "opt_updated":       None,
            "audit":             audit_base,
        }

    audit = dict(audit_base)
    audit["direct_bid"]          = quote.get("bid")
    audit["direct_ask"]          = quote.get("ask")
    audit["direct_quote_age_ms"] = quote.get("quote_age_ms")
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
    patched["_direct_quote_used"]   = True
    patched["_direct_quote_age_ms"] = quote.get("quote_age_ms")
    patched["_chain_bid"]           = chain_bid
    patched["_chain_ask"]           = chain_ask

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
      }

    Hard rejects on:
      - missing/invalid quote                → FINAL_CONTRACT_QUOTE_INVALID
      - bid <= 0 or ask <= 0 or ask < bid    → FINAL_CONTRACT_QUOTE_INVALID
      - spread_pct > max_spread_pct          → FINAL_SPREAD_TOO_WIDE
      - premium < min_premium                → FINAL_CONTRACT_QUOTE_INVALID
      - premium > max_premium                → FINAL_CONTRACT_UNAFFORDABLE
      - execution_price * 100 > budget_usd   → FINAL_CONTRACT_UNAFFORDABLE

    Never bypasses any of these for any reason.
    """
    _null_qf = {
        "final_bid":   None,
        "final_ask":   None,
        "final_mid":   None,
        "final_last":  None,
        "spread_pct":  None,
        "quote_age_ms": None,
    }

    quote = fetch_direct_option_quote(broker, contract, cache_ttl_s=0.0)
    if quote is None:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_CONTRACT_QUOTE_INVALID,
            "explanation": "final pre-submit quote unavailable",
            **_null_qf,
        }

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
        }

    bid_f, ask_f = float(bid), float(ask)
    mid = (bid_f + ask_f) / 2.0
    spread_pct = (ask_f - bid_f) / mid if mid > 0 else None

    qf = {
        "final_bid":    bid_f,
        "final_ask":    ask_f,
        "final_mid":    mid,
        "final_last":   last,
        "spread_pct":   spread_pct,
        "quote_age_ms": quote.get("quote_age_ms"),
    }

    if spread_pct is not None and spread_pct > max_spread_pct:
        return {
            "ok":          False,
            "reason_code": REASON_FINAL_SPREAD_TOO_WIDE,
            "explanation": f"spread {spread_pct*100:.1f}% > max {max_spread_pct*100:.1f}%",
            **qf,
        }

    premium_per_contract = mid * 100.0
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

    # FIX 3: validate full order cost (qty contracts) against budget.
    # execution_cost = execution_price * 100 * qty
    _qty = max(1, int(qty))
    execution_price = ask_f if is_live else mid
    execution_cost  = execution_price * 100.0 * _qty
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
