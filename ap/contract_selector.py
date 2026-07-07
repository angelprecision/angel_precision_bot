# ap/contract_selector.py
# PATCH: capital alignment — scoring stays midpoint; affordability/reserved risk uses ask execution basis.
#
# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL QUALITY GATE INVARIANT (do not remove)
# ══════════════════════════════════════════════════════════════════════════════
# contract_selector is the sole contract-quality authority for the queue path:
#   /signal → queue → worker_loop → master_control → contract_selector → watcher
#
# AUDIT 2026-05-17: The breach path (_on_entry_trigger in ap_execution_core.py)
# was historically documented as using evaluate_contract() from
# ap_options_intelligence.py. That is NO LONGER TRUE. The breach path calls
# self.contract_selector.select() (ap_execution_core.py ~L627) — the SAME
# engine as the queue path. evaluate_contract() is now dead code: it is not
# imported or called anywhere in the live system. There is therefore a SINGLE
# contract-quality gate (this file). No dual-gate equivalence burden remains.
# If evaluate_contract() is ever revived, it MUST import the thresholds from
# this module rather than redefining them.
# ══════════════════════════════════════════════════════════════════════════════ -- APContractSelectionEngine
# =============================================================================
# Unified contract selection. Takes an ApprovedExecutionPlan, returns the
# single best tradable contract + real sizing based on actual premium.
#
# Selection algorithm:
#   A. Earnings blackout gate (APEarningsGuard) -- before chain fetch
#   B. Choose expiration  (0DTE / weekly / nearest)
#   C. Filter to direction (CALL / PUT)
#   D. Hard quality filters (spread, OI, volume, DTE, delta)
#   E. IV rank gate (APIVRankFilter) -- after chain fetch
#   F. Rank survivors (delta fit, spread, OI, volume, premium fit)
#   G. Price sanity (buy-side: prefer ask when spread is tight)
#   H. Affordability → final contract count from real premium
#
# Replaces the placeholder:
#   max_position_usd = contracts * 100 * 5.0
# With:
#   real premium × qty × 100
# =============================================================================

from __future__ import annotations

import math
import logging
from ap.trace import trace_gate
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

# P0A: direct option quote revalidation (do not remove — restores flow for
# liquid tickers with stale/zero chain data without weakening safety rules).
from ap.contract_quote_revalidator import (
    revalidate_with_direct_quote  as _revalidate_direct,
    should_revalidate             as _should_revalidate,
    DEFAULT_REVALIDATE_TOP_N,
)

# ── P0: exact chain-fetch taxonomy exceptions ────────────────────────────────
class ChainProviderError(Exception):
    """HTTP or network error from the options chain provider."""
    def __init__(self, msg: str, *, status_code: int | None = None):
        super().__init__(msg)
        self.status_code = status_code


class ChainAuthError(ChainProviderError):
    """401/403 from the options chain provider — credential/token failure."""
    pass


class ChainEmptyExpirations(Exception):
    """Expirations endpoint returned an empty list for this ticker."""
    pass


class NoExpirationInDTEWindow(Exception):
    """Expirations exist but none fall within the configured DTE window."""
    pass


class ChainEmptyOptions(Exception):
    """Options chain endpoint returned zero rows for this expiration."""
    pass


class ChainParseEmpty(Exception):
    """Options chain response parsed to zero rows after direction filter."""
    pass

TICKER_MAX_PREMIUM_PER_CONTRACT = {
    "NVDA":  1500.0,  "TSLA": 1200.0,  "MSTR": 2000.0,
    "META":   800.0,  "MSFT":  800.0,  "AMZN":  800.0,
    "GOOGL":  800.0,  "NFLX":  800.0,  "AVGO":  800.0,
    "AAPL":   600.0,  "AMD":   600.0,  "CRM":   600.0,
    "SPY":    400.0,  "QQQ":   500.0,  "IWM":   300.0,
    "_DEFAULT": 800.0,
}

def _get_max_premium(ticker: str) -> float:
    return TICKER_MAX_PREMIUM_PER_CONTRACT.get(
        (ticker or "").upper(),
        TICKER_MAX_PREMIUM_PER_CONTRACT["_DEFAULT"]
    )

# =============================================================================
# REASON CODE NORMALIZER
# Maps internal free-form rejection strings → canonical observability codes
# so every reject lands in decision_events with a queryable, structured code.
# =============================================================================

_REASON_CODE_MAP: dict[str, str] = {
    # P1 selector honesty: zero-quote chain rows are CHAIN_ROW_ZERO_BID_ASK,
    # not NO_CHAIN_DATA. NO_CHAIN_DATA is reserved for empty/failed chain fetches.
    "zero_bid_or_ask":       "CHAIN_ROW_ZERO_BID_ASK",
    "ask_below_bid":         "CHAIN_ROW_ZERO_BID_ASK",
    "zero_mid":              "CHAIN_ROW_ZERO_BID_ASK",
    "reject_direct_zero":    "DIRECT_QUOTE_ZERO_BID_ASK",
    "reject_unavailable":    "DIRECT_QUOTE_UNAVAILABLE",
    "size_too_thin":         "VOLUME_TOO_LOW",
    "spread_too_wide":       "SPREAD_TOO_WIDE",
    "illiquid_vol":          "OI_TOO_LOW",
    # P1: bid_below min is a distinct gate from premium affordability
    "bid_below":             "BID_BELOW_MIN",
    "oi_too_low":            "OI_TOO_LOW",
    "volume_too_low":        "VOLUME_TOO_LOW",
    "dte_out_of_range":      "DTE_OUT_OF_RANGE",
    "delta_out_of_range":    "DELTA_OUT_OF_RANGE",
    "premium_too_high":      "PREMIUM_CAP_EXCEEDED",
    "moneyness_out_of_range": "DELTA_OUT_OF_RANGE",   # canonical: OTM moneyness = delta out of range
    "premium_too_low":        "NO_AFFORDABLE_CONTRACT",
    "low_oi":                 "OI_TOO_LOW",
    "low_volume":             "VOLUME_TOO_LOW",
    "dte_too_low":            "DTE_OUT_OF_RANGE",
    "dte_too_high":           "DTE_OUT_OF_RANGE",
    "invalid_expiration":     "DTE_OUT_OF_RANGE",
    "delta_out_of_band":      "DELTA_OUT_OF_RANGE",
}

def _normalize_reason_code(raw_reason: str) -> str:
    """
    Maps a free-form internal rejection string to a canonical observability
    reason code from REASON_CODES in ap/observability.py.
    Falls back gracefully so emit never crashes on unknown strings.
    """
    if not raw_reason:
        return "UNKNOWN_REJECTION"
    r = raw_reason.lower()
    for key, code in _REASON_CODE_MAP.items():
        if key in r:
            return code
    code = raw_reason.upper()
    if code not in _UNKNOWN_SELECTOR_REASONS_SEEN:
        _UNKNOWN_SELECTOR_REASONS_SEEN.add(code)
        log.warning("Unmapped selector rejection reason observed: %s", code)
    return code


# ── PR P1: queue/dashboard-facing reason mapping ─────────────────────────────
# Maps internal canonical reason codes → stable queue-facing codes written to
# trade_queue.last_error (via PR #183 write-back) and displayed in the dashboard.
# Observability only. No gate, threshold, or selection logic is touched.
# ─────────────────────────────────────────────────────────────────────────────
_TO_QUEUE_REASON: dict[str, str] = {
    # Chain-level fetch failures → generic NO_CHAIN_DATA for the queue layer
    # (distinguishes "data pipeline problem" from "data exists but bad quality")
    "CHAIN_EMPTY":                    "NO_CHAIN_DATA",
    "CHAIN_FETCH_FAILED":             "NO_CHAIN_DATA",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS": "NO_CHAIN_DATA",
    "QUOTE_FETCH_FAILED":             "QUOTE_FETCH_FAILED",
    # Zero-quote rows — data was returned but bid/ask is unusable
    "CHAIN_ROW_ZERO_BID_ASK":         "QUOTE_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK":      "QUOTE_ZERO_BID_ASK",
    # 1:1 pass-through — reason is already dashboard-safe and actionable
    "BID_BELOW_MIN":                  "BID_BELOW_MIN",
    "SPREAD_TOO_WIDE":                "SPREAD_TOO_WIDE",
    "OI_TOO_LOW":                     "OI_TOO_LOW",
    "VOLUME_TOO_LOW":                 "VOLUME_TOO_LOW",
    "DELTA_OUT_OF_RANGE":             "DELTA_OUT_OF_RANGE",
    "DTE_OUT_OF_RANGE":               "DTE_OUT_OF_RANGE",
    "NO_AFFORDABLE_CONTRACT":         "NO_AFFORDABLE_CONTRACT",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE":   "NO_AFFORDABLE_CONTRACT",
    "PREMIUM_CAP_EXCEEDED":           "PREMIUM_CAP_EXCEEDED",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT": "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    "NO_CONTRACT_AFTER_FILTERS":      "NO_CONTRACT_AFTER_FILTERS",
}


def _to_queue_reason(reason_code: str) -> str:
    """Map an internal canonical reason code to its stable queue/dashboard-facing code.

    Returns the reason_code unchanged when no mapping exists (unknown codes surface
    as-is rather than silently collapsing to a misleading generic bucket).
    """
    return _TO_QUEUE_REASON.get(str(reason_code or ""), str(reason_code or "UNKNOWN_REJECTION"))


def _attach_selector_failure(
    plan,
    *,
    reason_code: str,
    explanation: str,
    chain_rows: int = 0,
    survivor_count: int = 0,
    reject_buckets: "dict | None" = None,
    base_url: str = "",
    execution_mode: str = "unknown",
    best_rejected_candidate: "dict | None" = None,
    chain_quote_validity: "dict | None" = None,           # P0 PR #302 Fix 4
    direct_quote_recovery_audit: "dict | None" = None,   # P0 PR #302 Fix 2
) -> None:
    """Attach plan.metadata["selector_failure"] when select() returns None.

    PR P1 — selector reason honesty. Provides a structured, queryable failure
    dict so callers (e.g. PR #183 write-back in ap_execution_core) can surface
    truthful reason codes to trade_queue.last_error and the operator dashboard.

    Fields:
        reason_code        — canonical internal code (e.g. CHAIN_ROW_ZERO_BID_ASK)
        queue_reason_code  — stable dashboard-facing code (e.g. QUOTE_ZERO_BID_ASK)
        explanation        — human-readable detail
        chain_rows         — count of rows returned by the option chain fetch
        survivor_count     — count of contracts that survived quality filters
        quote_source       — tradier_live | tradier_sandbox | tradier_unknown | unknown
        chain_source       — always "tradier" for now
        tradier_base_url   — base URL from the data broker (for audit)
        sandbox_mode       — True when base_url contains "sandbox"
        execution_mode     — paper | live | unknown
        top_reject_buckets — {canonical_reason: count} sorted highest-count first

    Never raises. Observability only — does not affect any gate or decision.
    """
    try:
        _rc  = str(reason_code or "UNKNOWN_REJECTION")
        _qrc = _to_queue_reason(_rc)
        _base = str(base_url or "")
        if "api.tradier.com" in _base:
            _quote_src = "tradier_live"
        elif "sandbox" in _base.lower():
            _quote_src = "tradier_sandbox"
        elif _base:
            _quote_src = "tradier_unknown"
        else:
            _quote_src = "unknown"

        _top = {
            _normalize_reason_code(k): v
            for k, v in sorted(
                (reject_buckets or {}).items(), key=lambda x: -x[1]
            )
        }

        failure = {
            "reason_code":        _rc,
            "queue_reason_code":  _qrc,
            "explanation":        str(explanation or ""),
            "chain_rows":         int(chain_rows or 0),
            "survivor_count":     int(survivor_count or 0),
            "quote_source":       _quote_src,
            "chain_source":       "tradier",
            "tradier_base_url":   _base,
            "sandbox_mode":       "sandbox" in _base.lower(),
            "execution_mode":     str(execution_mode or "unknown").lower(),
            "top_reject_buckets": _top,
            # P0 (PR #299): named per-bucket counts extracted from top_reject_buckets
            # so dashboards can query specific fields without parsing the dict.
            "rejected_by_oi":          _top.get("OI_TOO_LOW", 0),
            "rejected_by_spread":      _top.get("SPREAD_TOO_WIDE", 0),
            "rejected_by_volume":      _top.get("VOLUME_TOO_LOW", 0),
            "rejected_by_zero_bid_ask": (
                _top.get("CHAIN_ROW_ZERO_BID_ASK", 0)
                + _top.get("DIRECT_QUOTE_ZERO_BID_ASK", 0)
            ),
            # P0 (PR #299): best contract that failed quality gates — proves there
            # was (or wasn't) a real near-ATM option available at breach time.
            # Operators can answer "was there ANY liquid contract?" without logs.
            "best_rejected_candidate": best_rejected_candidate or None,
        }

        # P0 PR #302 Fix 3: failure classification
        try:
            _fclass, _dfail, _qfail = _classify_selector_failure(
                _rc,
                chain_quote_validity=chain_quote_validity,
                sandbox_mode="sandbox" in _base.lower(),
                execution_mode=str(execution_mode or "unknown"),
            )
            failure["selector_failure_class"]   = _fclass
            failure["selector_reason_detail"]   = str(explanation or "")
            failure["selector_terminal_reason"] = _rc
            failure["data_failure"]             = _dfail
            failure["quality_failure"]          = _qfail
        except Exception:
            pass

        # P0 PR #302 Fix 4: chain quote validity summary
        if chain_quote_validity:
            failure["selector_chain_quote_validity"] = chain_quote_validity

        # P0 PR #302 Fix 2: direct quote recovery audit
        if direct_quote_recovery_audit:
            failure.update({
                "direct_quote_recovery_attempted": bool(
                    direct_quote_recovery_audit.get("attempted", False)),
                "direct_quote_recovery_selected":  bool(
                    direct_quote_recovery_audit.get("selected", False)),
                "direct_quote_recovery_contract":  direct_quote_recovery_audit.get("contract"),
                "direct_quote_recovery_bid":       direct_quote_recovery_audit.get("bid"),
                "direct_quote_recovery_ask":       direct_quote_recovery_audit.get("ask"),
                "direct_quote_recovery_mid":       direct_quote_recovery_audit.get("mid"),
                "direct_quote_recovery_failure":   direct_quote_recovery_audit.get("failure"),
            })

        if isinstance(plan, dict):
            plan.setdefault("metadata", {})["selector_failure"] = failure
        else:
            meta = getattr(plan, "metadata", None)
            if not isinstance(meta, dict):
                meta = {}
                try:
                    setattr(plan, "metadata", meta)
                except Exception:
                    pass
            if isinstance(meta, dict):
                meta["selector_failure"] = failure
    except Exception:
        pass  # observability must never interrupt select()


# ── P0 PR #302 — Fix 4: Chain quote validity summary ─────────────────────────

def _build_chain_quote_validity(chain_rows: list) -> dict:
    """
    Build a validity summary of the option chain before quality filtering.
    Distinguishes data-quality failures (zero quotes from Tradier) from
    contract-quality failures (real quotes that don't pass thresholds).
    Never raises — returns empty validity dict on any error.
    """
    try:
        n = len(chain_rows)
        if n == 0:
            return {
                "chain_rows": 0,
                "rows_with_bid_gt_zero": 0,
                "rows_with_ask_gt_zero": 0,
                "rows_with_bid_and_ask_gt_zero": 0,
                "rows_zero_bid_or_ask": 0,
                "zero_quote_ratio": 1.0,
                "nonzero_quote_ratio": 0.0,
                "best_nonzero_quote_candidate": None,
                "best_zero_quote_candidate": None,
            }
        _bid = lambda o: float(o.get("bid") or 0)
        _ask = lambda o: float(o.get("ask") or 0)
        bid_gt  = sum(1 for o in chain_rows if _bid(o) > 0)
        ask_gt  = sum(1 for o in chain_rows if _ask(o) > 0)
        both_gt = sum(1 for o in chain_rows if _bid(o) > 0 and _ask(o) > 0)
        zero_ba = n - both_gt
        nonzero_cands = [o for o in chain_rows if _bid(o) > 0 and _ask(o) > 0]
        zero_cands    = [o for o in chain_rows if _bid(o) <= 0 or _ask(o) <= 0]
        best_nonzero = None
        if nonzero_cands:
            _best = max(nonzero_cands, key=lambda o: (_bid(o) + _ask(o)) / 2)
            best_nonzero = {
                "symbol": _best.get("symbol"),
                "bid":    round(_bid(_best), 4),
                "ask":    round(_ask(_best), 4),
                "mid":    round((_bid(_best) + _ask(_best)) / 2, 4),
                "open_interest": int(_best.get("open_interest") or 0),
                "delta": _best.get("greeks", {}).get("delta") if isinstance(_best.get("greeks"), dict) else None,
            }
        best_zero = None
        if zero_cands:
            _bzero = max(zero_cands, key=lambda o: int(o.get("open_interest") or 0))
            best_zero = {
                "symbol":        _bzero.get("symbol"),
                "bid":           round(_bid(_bzero), 4),
                "ask":           round(_ask(_bzero), 4),
                "open_interest": int(_bzero.get("open_interest") or 0),
            }
        return {
            "chain_rows":                     n,
            "rows_with_bid_gt_zero":           bid_gt,
            "rows_with_ask_gt_zero":           ask_gt,
            "rows_with_bid_and_ask_gt_zero":   both_gt,
            "rows_zero_bid_or_ask":            zero_ba,
            "zero_quote_ratio":                round(zero_ba / n, 4),
            "nonzero_quote_ratio":             round(both_gt / n, 4),
            "best_nonzero_quote_candidate":    best_nonzero,
            "best_zero_quote_candidate":       best_zero,
        }
    except Exception:
        return {}


# ── P0 PR #302 — Fix 3: Selector failure classification ──────────────────────

def _classify_selector_failure(
    reason_code: str,
    chain_quote_validity: "dict | None" = None,
    sandbox_mode: bool = False,
    execution_mode: str = "unknown",
) -> "tuple[str, bool, bool]":
    """
    Classify a selector failure into:
      selector_failure_class: "data_quality_zero_quotes" |
                              "contract_quality_reject"  |
                              "paper_data_domain_issue"
      data_failure:    True when the blocker is data quality (zero quotes,
                       sandbox data, chain empty) not contract quality.
      quality_failure: True when a real contract failed a threshold gate.

    Used by operators to distinguish 'market data missing' from 'no good contracts'.
    """
    _rc  = str(reason_code or "").upper()
    _qty = chain_quote_validity or {}
    _zero_ratio = float(_qty.get("zero_quote_ratio") or 0.0)
    _both_gt    = int(_qty.get("rows_with_bid_and_ask_gt_zero") or 0)

    if _rc == "PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE":
        return "paper_data_domain_issue", True, False

    if _rc in _QUALITY_REJECT_CODES:
        return "contract_quality_reject", False, True

    if _rc in ("CHAIN_ROW_ZERO_BID_ASK", "DIRECT_QUOTE_ZERO_BID_ASK",
               "QUOTE_ZERO_BID_ASK", "CHAIN_PROVIDER_EMPTY_OPTIONS",
               "CHAIN_PROVIDER_EMPTY_EXPIRATIONS", "NO_CHAIN_DATA",
               "CHAIN_PROVIDER_ERROR", "CHAIN_AUTH_ERROR"):
        return "data_quality_zero_quotes", True, False

    # Mixed: some valid rows but quality gates failed
    if _both_gt > 0:
        return "contract_quality_reject", False, True

    return "data_quality_zero_quotes", True, False


# ── P0 PR #302 — Fix 1: paper domain utilities ────────────────────────────────

def _is_paper_sandbox_data_failure(reason_code: str) -> bool:
    """True when the reject reason indicates sandbox data quality, not contract quality."""
    return str(reason_code or "").upper() in _PAPER_SANDBOX_DATA_FAILURE_CODES


def _detect_paper_selector_domain(sel_mode: str, sel_base_url: str) -> str:
    """
    Classify the effective data domain for the selector.
    Returns: "live" | "sandbox" | "unknown"
    Respects PAPER_SELECTOR_MARKET_DATA_DOMAIN env var for override.
    """
    _url = str(sel_base_url or "").lower()
    if _PAPER_SELECTOR_MARKET_DATA_DOMAIN == "live":
        return "live"
    if _PAPER_SELECTOR_MARKET_DATA_DOMAIN == "sandbox":
        return "sandbox"
    # auto: infer from base_url
    if "api.tradier.com" in _url:
        return "live"
    if "sandbox" in _url:
        return "sandbox"
    return "unknown"


def _safe_float(val, default: float = 0.0) -> float:
    """Coerce any value to float safely — never raises."""
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _extract_abs_delta(opt: dict) -> tuple[Optional[float], str]:
    """
    Safely extract absolute delta from Tradier greeks dict.

    CRITICAL: Missing/dirty Greeks must NEVER fall back to target_delta.
    That silent fallback is what allowed the $0.11 far-OTM contract to score
    as well as a $0.50 contract with real Greeks (bug root cause).

    Returns (abs_delta, reason).  abs_delta is None when unavailable/corrupt.
    """
    greeks = opt.get("greeks")
    if not greeks or not isinstance(greeks, dict):
        return None, "missing_dirty_greeks_not_dict"
    raw = greeks.get("delta")
    if raw is None or raw == "":
        return None, "missing_delta"
    try:
        d = abs(float(raw))
    except Exception:
        return None, f"dirty_delta_{raw}"
    if d <= 0 or d > 1.0:
        return None, f"delta_out_of_valid_range_{d:.4f}"
    return d, "ok"


def _extract_iv(opt: dict) -> Optional[float]:
    """Best-effort IV extraction for observability payloads."""
    for _key in ("iv", "implied_volatility", "impliedVolatility"):
        _val = opt.get(_key)
        if _val not in (None, ""):
            try:
                return round(float(_val), 4)
            except Exception:
                pass
    greeks = opt.get("greeks")
    if isinstance(greeks, dict):
        for _key in ("iv", "smv_vol", "mid_iv"):
            _val = greeks.get(_key)
            if _val not in (None, ""):
                try:
                    return round(float(_val), 4)
                except Exception:
                    pass
    return None


def _extract_option_type(opt: dict) -> str:
    """Return canonical CALL/PUT label for observability payloads."""
    raw = (
        opt.get("option_type")
        or opt.get("type")
        or opt.get("put_call")
        or opt.get("right")
        or ""
    )
    label = str(raw).strip().upper()
    if label in {"C", "CALL"}:
        return "CALL"
    if label in {"P", "PUT"}:
        return "PUT"
    return ""


def _round_or_none(value, digits=4):
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except Exception:
        return None


def _build_candidate_row(
    opt: dict,
    *,
    rank_score: Optional[float],
    rejected_at_step: Optional[str],
    rejection_reason: Optional[str],
    selected: bool,
) -> dict:
    bid = _safe_float(opt.get("bid"))
    ask = _safe_float(opt.get("ask"))
    mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else _safe_float(opt.get("mid"))
    spread_pct = ((ask - bid) / mid) if mid > 0 else None
    premium = mid * 100 if mid > 0 else None
    abs_delta, _ = _extract_abs_delta(opt)
    return {
        "symbol": opt.get("symbol", "?"),
        "strike": _round_or_none(opt.get("strike")),
        "expiration": opt.get("expiration_date") or opt.get("expiration") or "",
        "option_type": _extract_option_type(opt),
        "delta": _round_or_none(abs_delta),
        "iv": _extract_iv(opt),
        "bid": _round_or_none(bid),
        "ask": _round_or_none(ask),
        "mid": _round_or_none(mid),
        "spread_pct": _round_or_none(spread_pct),
        "open_interest": int(_safe_float(opt.get("open_interest"))),
        "volume": int(_safe_float(opt.get("volume"))),
        "premium": _round_or_none(premium, 2),
        "rank_score": _round_or_none(rank_score),
        "rejected_at_step": rejected_at_step,
        "rejection_reason": rejection_reason,
        "selected": bool(selected),
    }


def _build_candidate_audit(scored, underlying_price, selected_symbol, selected_reason, rejections, top_n=3):
    """Build compact selector candidate persistence payload (observability only).

    scored: list of (rank_score, opt_dict) already sorted best-first.
    Returns a dict safe to drop into orders.meta. Never raises.
    """
    candidates = []
    selected_winner = None
    try:
        for _rank, (_s, _c) in enumerate(scored[:top_n]):
            _selected = bool(selected_symbol) and _c.get("symbol") == selected_symbol
            _row = _build_candidate_row(
                _c,
                rank_score=_s,
                rejected_at_step=None if _selected else "ranking",
                rejection_reason=None if _selected else "ranked_below_selected",
                selected=_selected,
            )
            _row["rank"] = _rank + 1
            _row["contract"] = _row["symbol"]
            candidates.append(_row)
            if _selected:
                selected_winner = dict(_row)
    except Exception:
        pass

    rejected = {}
    try:
        rejected = dict(sorted((rejections or {}).items(), key=lambda x: -x[1]))
    except Exception:
        rejected = {}

    return {
        "schema_version":            2,
        "selected_contract":          selected_symbol,
        "selected_reason":            selected_reason,
        "selected_winner":            selected_winner,
        "underlying_price":           round(_safe_float(underlying_price), 4),
        "candidates_considered":      len(scored) if scored else 0,
        "candidate_cap":              top_n,
        "candidates":                 candidates,
        "hard_filter_rejects":        rejected,
        # Backward-compatible aliases for existing readers/tests.
        "top_candidates":             candidates,
        "rejected_candidate_reasons": rejected,
    }


def _safe_plan_attr(plan, attr: str, default=None):
    """Get attribute from plan whether it is an object or dict."""
    try:
        return getattr(plan, attr, default)
    except Exception:
        pass
    try:
        return plan.get(attr, default) if isinstance(plan, dict) else default
    except Exception:
        return default


def _plan_sizing_ctx(plan) -> dict:
    """Return plan.metadata['sizing_context'] (object or dict plan), or {}.

    Production plans store equity / budget / risk_pct / max_position_usd under
    metadata['sizing_context'] — NOT as top-level plan attributes. Reading the
    top-level attrs returns None in production, which defeats the account-size
    diagnostics. This helper reads the canonical location. Never raises."""
    try:
        meta = None
        if isinstance(plan, dict):
            meta = plan.get("metadata")
        else:
            meta = getattr(plan, "metadata", None)
        if isinstance(meta, dict):
            ctx = meta.get("sizing_context")
            if isinstance(ctx, dict):
                return ctx
    except Exception:
        pass
    return {}


def _sizing_val(plan, *keys, default=None):
    """Read the first present value from sizing_context for any of `keys`,
    falling back to a same-named top-level plan attr, then default. Never raises."""
    ctx = _plan_sizing_ctx(plan)
    for k in keys:
        if k in ctx and ctx[k] is not None:
            return ctx[k]
    for k in keys:
        v = _safe_plan_attr(plan, k, None)
        if v is not None:
            return v
    return default





from ap.observability import emit_decision_event, get_git_commit, make_config_hash
log = logging.getLogger("ap.contract_selector")


# =============================================================================
# PRO-LEVEL CONTRACT QUALITY THRESHOLDS
# =============================================================================

_PRO_QUALITY_ENABLED = os.getenv("PRO_CONTRACT_QUALITY", "true").lower() != "false"

# P0 PR #302 — Fix 1: paper selector market-data domain config.
# auto = detect from base_url; live = require live; sandbox = allow sandbox.
_PAPER_SELECTOR_MARKET_DATA_DOMAIN = os.getenv(
    "PAPER_SELECTOR_MARKET_DATA_DOMAIN", "auto"
).strip().lower()

# P0 PR #302 — Fix 2: DIRECT_QUOTE_RECOVERY_TOP_N env alias.
# Maps to existing CONTRACT_REVALIDATE_TOP_N; preference given to the new name.
_DIRECT_QUOTE_RECOVERY_TOP_N = int(
    os.getenv("DIRECT_QUOTE_RECOVERY_TOP_N",
              os.getenv("CONTRACT_REVALIDATE_TOP_N", str(DEFAULT_REVALIDATE_TOP_N)))
)

# Paper sandbox data failure reasons — these indicate data domain issues,
# not true contract quality failures.
_PAPER_SANDBOX_DATA_FAILURE_CODES = frozenset({
    "CHAIN_ROW_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "CHAIN_PROVIDER_EMPTY_OPTIONS",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
    "QUOTE_ZERO_BID_ASK",
})

# Failure classification mapping — determines data_failure vs quality_failure.
_QUALITY_REJECT_CODES = frozenset({
    "OI_TOO_LOW", "SPREAD_TOO_WIDE", "VOLUME_TOO_LOW",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE", "DEEP_OTM_REJECT", "OTM_TOO_FAR",
    "PREMIUM_TOO_HIGH", "PREMIUM_TOO_LOW", "LIQUIDITY_BELOW_THRESHOLD",
    "FINAL_SPREAD_TOO_WIDE", "FINAL_CONTRACT_UNAFFORDABLE",
})

# Safety defaults:
# - Guard subsystem errors fail closed unless explicitly overridden.
# - Budget clipping is explicit and observable so upstream sizing does not silently disagree.
_RAW_SELECTOR_FAIL_OPEN_GUARD_ERRORS = os.getenv("SELECTOR_FAIL_OPEN_GUARD_ERRORS", "").strip().upper()
_SELECTOR_FAIL_OPEN_GUARD_ERRORS = (_RAW_SELECTOR_FAIL_OPEN_GUARD_ERRORS == "I_UNDERSTAND_THIS_IS_UNSAFE")
_QUALITY_RULES_VERSION = os.getenv("CONTRACT_QUALITY_RULES_VERSION", "contract_selector_v1")
_UNKNOWN_SELECTOR_REASONS_SEEN: set[str] = set()

if _SELECTOR_FAIL_OPEN_GUARD_ERRORS:
    log.critical(
        "SELECTOR_FAIL_OPEN_GUARD_ERRORS OVERRIDE ENABLED — guard exceptions will FAIL-OPEN. "
        "This is unsafe for production and should not be enabled for live trading."
    )

def _max_trade_usd() -> float:
    # 10% of account per trade. At $17,767 equity = $1,776 max.
    # Env var MAX_TRADE_USD overrides (set in Render for live clients).
    try:
        return float(os.getenv("MAX_TRADE_USD", "1800"))
    except Exception:
        return 1800.0

def _effective_budget(raw_budget: float) -> tuple[float, float, bool]:
    cap = _max_trade_usd()
    try:
        budget = float(raw_budget or 0)
    except Exception:
        budget = 0.0
    eff = min(budget, cap)
    return eff, cap, bool(budget > cap)

def _pricing_basis_for_mode(mode: str) -> tuple[bool, str]:
    """Return (is_live, pricing_basis) for selector capital math."""
    is_live = str(mode or "paper").upper() == "LIVE"
    return is_live, "ASK_EXECUTION" if is_live else "MID_SIMULATION"

_PRO_TIER1_TICKERS = {
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "NVDA", "AMD", "META", "GOOG", "GOOGL",
    "TSLA", "AMZN", "NFLX",
}

_PRO_MIN_BID            = 0.10
_PRO_MIN_BID_SIZE_HARD  = 3

_PRO_T1_SPREAD_HARD_MAX = 0.10   # 10% — T1 tickers (AAPL/NVDA/TSLA/etc)
_PRO_T1_SPREAD_A_TIER   = 0.05   # 5%  — A-tier quality threshold
_PRO_T1_SIZE_MIN        = 5

_PRO_T2_SPREAD_HARD_MAX = 0.12   # 12% — T2 tickers (everything else)
_PRO_T2_SPREAD_A_TIER   = 0.06   # 6%  — A-tier quality threshold
_PRO_T2_SIZE_MIN        = 3


def _pro_contract_quality(opt: dict, ticker: str, dte: int) -> tuple[str, str]:
    is_t1 = ticker.upper() in _PRO_TIER1_TICKERS

    bid = float(opt.get("bid") or 0)
    ask = float(opt.get("ask") or 0)
    vol = int(opt.get("volume") or 0)
    oi  = int(opt.get("open_interest") or 0)

    bid_size = int(opt.get("bid_size") or opt.get("bidsize") or 0)
    ask_size = int(opt.get("ask_size") or opt.get("asksize") or 0)

    if bid <= 0 or ask <= 0:
        return "REJECT", "zero_bid_or_ask"
    if ask < bid:
        return "REJECT", "ask_below_bid"
    if bid < _PRO_MIN_BID:
        return "REJECT", f"bid_below_{_PRO_MIN_BID}"

    mid = (bid + ask) / 2
    if mid <= 0:
        return "REJECT", "zero_mid"
    spread_pct = (ask - bid) / mid

    hard_spread = _PRO_T1_SPREAD_HARD_MAX if is_t1 else _PRO_T2_SPREAD_HARD_MAX
    a_spread    = _PRO_T1_SPREAD_A_TIER  if is_t1 else _PRO_T2_SPREAD_A_TIER

    if spread_pct > hard_spread:
        return "REJECT", f"spread_too_wide_{spread_pct*100:.1f}%_max_{hard_spread*100:.0f}%"

    # P0 (monday-trade-flow-readiness) — one-sided-size trap fix.
    # Tradier chain rows frequently report size on only ONE side (e.g.
    # bid_size=12, ask_size=0). The previous gate `if bid_size or ask_size:`
    # then rejected such rows as size_too_thin even when the reported side was
    # deep and the quote was live/tight — a data-shape artifact, not thinness.
    # The hard size gate now applies ONLY when BOTH sides report a size, so a
    # genuinely thin two-sided book still rejects, while one-sided reporting
    # falls through to the vol/OI liquidity gate below (which still protects).
    if bid_size and ask_size:
        if bid_size < _PRO_MIN_BID_SIZE_HARD or ask_size < _PRO_MIN_BID_SIZE_HARD:
            return "REJECT", f"size_too_thin_bid{bid_size}_ask{ask_size}"

    if dte <= 1:
        min_vol, min_oi = 50,  500    # 0–1 DTE: OI=500 (user spec)
    elif dte <= 7:
        min_vol, min_oi = 100, 500    # 2–7 DTE: OI 1000→500, vol 200→100
    else:
        min_vol, min_oi = 150, 1000   # 8+ DTE: OI 2000→1000

    if vol < min_vol and oi < min_oi:
        return "REJECT", f"illiquid_vol{vol}_oi{oi}_need_v{min_vol}_or_oi{min_oi}"

    size_ok_A = (bid_size >= _PRO_T1_SIZE_MIN and ask_size >= _PRO_T1_SIZE_MIN) if is_t1                 else (bid_size >= _PRO_T2_SIZE_MIN and ask_size >= _PRO_T2_SIZE_MIN)
    has_size  = bool(bid_size or ask_size)
    strong_liq = (vol >= min_vol * 2) or (oi >= min_oi * 2)

    if spread_pct <= a_spread and strong_liq and (size_ok_A or not has_size):
        return "A", "tight_liquid"

    return "B", "ok_liquid"


# =============================================================================
# OUTPUT DATACLASS
# =============================================================================

@dataclass
class SelectedContract:
    contract_symbol:       str
    expiration:            str
    strike:                float
    option_type:           str
    bid:                   float
    ask:                   float
    mid:                   float
    spread_pct:            float
    delta:                 Optional[float]
    open_interest:         int
    volume:                int
    premium_per_share:     float
    premium_per_contract:  float
    affordable_contracts:  int
    selection_reason:      str
    selection_score:       float
    dte:                   int
    scoring_price_per_share: float = 0.0
    execution_price_per_share: float = 0.0
    effective_budget: float = 0.0
    budget_clipped: bool = False
    pricing_basis: str = ""
    # Item 3 — selector candidate audit (EVIDENCE ONLY, no behavior change).
    # Top-N candidates considered, each with bid/ask/mid/last/spread/delta/dte/
    # strike/moneyness/volume/OI + rank, plus selected_reason and the rejected
    # candidate reasons. None when not built. Persisted into orders.meta by the
    # execution layer so we can answer "why this contract and not that one?".
    candidate_audit: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "contract_symbol":      self.contract_symbol,
            "expiration":           self.expiration,
            "strike":               self.strike,
            "option_type":          self.option_type,
            "bid":                  self.bid,
            "ask":                  self.ask,
            "mid":                  self.mid,
            "spread_pct":           self.spread_pct,
            "delta":                self.delta,
            "open_interest":        self.open_interest,
            "volume":               self.volume,
            "premium_per_share":    self.premium_per_share,
            "premium_per_contract": self.premium_per_contract,
            "affordable_contracts": self.affordable_contracts,
            "selection_reason":     self.selection_reason,
            "selection_score":      self.selection_score,
            "dte":                  self.dte,
            "scoring_price_per_share": self.scoring_price_per_share,
            "execution_price_per_share": self.execution_price_per_share,
            "effective_budget": self.effective_budget,
            "budget_clipped": self.budget_clipped,
            "pricing_basis": self.pricing_basis,
            "candidate_audit": self.candidate_audit,
        }


# =============================================================================
# CONTRACT SELECTION ENGINE
# =============================================================================

class APContractSelectionEngine:

    def __init__(
        self,
        broker,
        *,
        mode:           str   = "paper",
        data_broker     = None,
        target_delta:   float = 0.40,
        delta_band:     float = 0.30,
        max_spread_pct: float = 0.50,
        min_oi:         int   = 1,
        min_volume:     int   = 0,
        min_premium:    float = 10.0,
        max_premium:    float = 350.0,
        max_dte:        int   = 21,
        min_dte:        int   = 0,
        prefer_weekly:  bool  = True,
        earnings_guard=None,
        iv_filter=None,
        mutate_plan: bool = True,
    ):
        self.broker         = broker
        self.data_broker    = data_broker if data_broker is not None else broker
        self.mode           = mode
        self.target_delta   = target_delta
        self.delta_band     = delta_band
        self.max_spread_pct = float(os.getenv("MAX_SPREAD_PCT", str(max_spread_pct)))
        self.min_oi         = min_oi
        self.min_volume     = min_volume
        self.min_premium    = min_premium
        self.max_premium    = max_premium
        self.max_dte        = max_dte
        self.min_dte        = min_dte
        self.prefer_weekly  = prefer_weekly
        self.earnings_guard = earnings_guard
        self.iv_filter      = iv_filter
        self.mutate_plan    = bool(mutate_plan)

        # ── PR1 (deferred-dte-ladder): DTE-bucket selection policy ────────────
        # AMENDMENT (PR #219, Jason LIVE recovery): default is now "1" (ON).
        # The ladder is already narrowly gated by _is_ladder_eligible(plan) —
        # only fires when plan.metadata["deferred_breach_selection"] is True,
        # a marker set exclusively by ap_execution_core's breach-time path.
        # Non-deferred select() calls are byte-for-byte unchanged. Flipping
        # the default from "0" to "1" enables the ladder for the exact path
        # that needed it (54 Jason LIVE overnight setups today expired without
        # a single broker submit because they all tried only one expiration).
        # The env var is preserved as an emergency kill switch: set
        # DEFERRED_DTE_LADDER=0 on Render to disable without a code deploy.
        self.dte_ladder_enabled = os.getenv("DEFERRED_DTE_LADDER", "1").strip() in ("1", "true", "yes")
        # Bucket boundaries (inclusive upper, DTE). A=near, B=adjacent, C=fallback.
        self.dte_bucket_a_max = int(os.getenv("DTE_BUCKET_A_MAX", "2"))   # 0–2 DTE
        self.dte_bucket_b_max = int(os.getenv("DTE_BUCKET_B_MAX", "7"))   # 3–7 DTE
        # Max expirations to probe per bucket (bounds Tradier calls per breach).
        self.dte_ladder_probe_per_bucket = int(os.getenv("DTE_LADDER_PROBE_PER_BUCKET", "2"))
        # Records the last ladder run for diagnostics (observability only).
        self._last_dte_ladder_audit: Optional[dict] = None

        # PR #149 — Selector Reason Honesty.
        # Records the most recent REJECT event emitted during a single select()
        # call so the queue can surface the actual blocker (e.g. OI_TOO_LOW)
        # instead of the umbrella label "no_contract_found".
        #   Shape: {"stage": str, "reason_code": str, "explanation": str} | None
        # Reset to None at the top of every select() invocation.
        # Never affects selection — observability only.
        self._last_failure: Optional[dict] = None

        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit       = get_git_commit()
        self.config_hash = make_config_hash({
            "mode":                   mode,
            "target_delta":           self.target_delta,
            "delta_band":             self.delta_band,
            "max_spread_pct":         self.max_spread_pct,
            "min_oi":                 self.min_oi,
            "min_volume":             self.min_volume,
            "min_premium":            self.min_premium,
            "max_premium":            self.max_premium,
            "min_dte":                self.min_dte,
            "max_dte":                self.max_dte,
            "prefer_weekly":          self.prefer_weekly,
            "pro_quality_enabled":    _PRO_QUALITY_ENABLED,
            "guard_error_fail_open":   _SELECTOR_FAIL_OPEN_GUARD_ERRORS,
            "quality_rules_version":   _QUALITY_RULES_VERSION,
            "mutate_plan":             self.mutate_plan,
            "pro_min_bid":            _PRO_MIN_BID,
            "pro_min_bid_size_hard":  _PRO_MIN_BID_SIZE_HARD,
            "pro_t1_spread_hard_max": _PRO_T1_SPREAD_HARD_MAX,
            "pro_t1_spread_a_tier":   _PRO_T1_SPREAD_A_TIER,
            "pro_t1_size_min":        _PRO_T1_SIZE_MIN,
            "pro_t2_spread_hard_max": _PRO_T2_SPREAD_HARD_MAX,
            "pro_t2_spread_a_tier":   _PRO_T2_SPREAD_A_TIER,
            "pro_t2_size_min":        _PRO_T2_SIZE_MIN,
            "min_contract_delta":     float(os.getenv("MIN_CONTRACT_DELTA", "0.10")),
            "max_otm_pct":            float(os.getenv("MAX_OTM_PCT", "0.12")),
            "max_trade_usd":          float(os.getenv("MAX_TRADE_USD", "500")),
            "max_ideal_premium":      float(os.getenv("MAX_IDEAL_PREMIUM", "3.50")),
            "ticker_premium_caps":    TICKER_MAX_PREMIUM_PER_CONTRACT,
        })

        log.info(
            "APContractSelectionEngine | mode=%s delta=%.2f±%.2f "
            "max_spread=%d%% dte=[%d,%d] premium=[$%.0f,$%.0f] "
            "earnings_guard=%s iv_filter=%s",
            mode, target_delta, delta_band,
            int(max_spread_pct * 100), min_dte, max_dte,
            min_premium, max_premium,
            type(earnings_guard).__name__ if earnings_guard is not None else "None",
            type(iv_filter).__name__ if iv_filter is not None else "None",
        )


    def _emit_selector_event(
        self,
        plan,
        stage: str,
        decision: str,
        reason_code: Optional[str],
        explanation: str,
        contract: Optional[str] = None,
        inputs: Optional[dict] = None,
        thresholds: Optional[dict] = None,
        context: Optional[dict] = None,
    ) -> None:
        """Emit a structured observability event for every contract selection decision."""
        # PR #149 — Selector Reason Honesty.
        # Capture the most recent REJECT into self._last_failure so the queue
        # can read the specific blocker after select() returns None. Always
        # overwrites: by the time select() returns None, the last REJECT
        # emitted is the actual terminating blocker. Errors here must not
        # affect emit behavior — wrapped defensively. Observability only.
        try:
            if str(decision).upper() == "REJECT":
                self._last_failure = {
                    "stage":       str(stage or ""),
                    "reason_code": str(reason_code or "") or "UNKNOWN_REJECTION",
                    "explanation": str(explanation or ""),
                }
        except Exception:
            pass
        try:
            _sig_id    = _safe_plan_attr(plan, "signal_id") or ""
            _client_id = _safe_plan_attr(plan, "client_id") or "default"
            emit_decision_event(
                run_id=os.getenv("AP_RUN_ID", "unknown"),
                candidate_id=str(_sig_id),
                client_id=_client_id,
                stage=stage,
                decision=decision,
                reason_code=reason_code,
                explanation=explanation,
                symbol=getattr(plan, "ticker", None) or (plan.get("ticker") if isinstance(plan, dict) else None),
                contract=contract,
                setup_type=getattr(plan, "pattern", None),
                timeframe=getattr(plan, "timeframe", None),
                strategy_version=self.strategy_version,
                config_hash=self.config_hash,
                git_commit=self.git_commit,
                inputs=inputs or {},
                thresholds=thresholds or {},
                context=context or {},
            )
        except Exception as e:
            log.debug("Selector observability emit failed (non-critical): %s", e)

    # =========================================================================
    # PR #149 — PUBLIC: get_last_failure()
    # =========================================================================
    def get_last_failure(self) -> Optional[dict]:
        """
        Return the most recent REJECT captured during the last select() call,
        or None if select() either succeeded or was never run.

        Shape:
            {"stage": "<selector stage>", "reason_code": "<canonical code>",
             "explanation": "<human-readable detail>"}

        Used by ap/queue.py to surface the *actual* blocker (e.g. OI_TOO_LOW,
        NO_CHAIN_DATA, SPREAD_TOO_WIDE, NO_AFFORDABLE_CONTRACT) instead of
        the umbrella label "no_contract_found".

        Observability only. Never affects selection. Never raises.
        """
        try:
            if isinstance(self._last_failure, dict):
                # Defensive copy so callers can't mutate selector state.
                return dict(self._last_failure)
        except Exception:
            pass
        return None

    def get_last_dte_ladder_audit(self) -> Optional[dict]:
        """Return the audit from the most recent DTE-ladder run (buckets tried,
        survivors per bucket, selected bucket/DTE/expiration), or None if the
        ladder was not used. Observability only. Never raises."""
        try:
            if isinstance(self._last_dte_ladder_audit, dict):
                return dict(self._last_dte_ladder_audit)
        except Exception:
            pass
        return None


    def select(self, plan, *, expiration_override: Optional[str] = None) -> Optional[SelectedContract]:
        # PR #149 — Selector Reason Honesty.
        # Reset failure capture at the start of every invocation so that
        # get_last_failure() reflects ONLY this call's most recent REJECT,
        # never a stale value from a prior select(). Observability only.
        self._last_failure = None

        # PR P1 — selector failure metadata tracking (observability only).
        # Updated at each stage and passed to _attach_selector_failure() at
        # every return-None site so callers see a truthful, structured reason.
        _sel_chain_rows:  int  = 0        # set after chain fetch
        _sel_survivors:   int  = 0        # set after quality filter
        _sel_rejections:  dict = {}        # set after quality filter
        _best_rejected_candidate: dict | None = None  # PR #299: best contract that failed quality gates
        _sel_base_url:    str  = (         # for sandbox_mode / quote_source
            str(getattr(getattr(self, "data_broker", None), "base_url", "") or
                getattr(getattr(self, "broker",      None), "base_url", "") or "")
        )
        _sel_mode: str = str(getattr(self, "mode", "unknown") or "unknown").lower()

        ticker    = _safe_plan_attr(plan, "ticker")
        direction = str(_safe_plan_attr(plan, "side", "") or "").upper()
        budget    = float(_safe_plan_attr(plan, "max_position_usd", 0) or 0)

        if not ticker or direction not in {"CALL", "PUT"}:
            self._emit_selector_event(
                plan,
                stage="selector_entry",
                decision="REJECT",
                reason_code="INVALID_PLAN",
                explanation=f"Invalid plan inputs: ticker={ticker}, side={direction}",
                inputs={"ticker": ticker, "side": direction, "budget": budget},
            )
            return None

        _LIQUID_ETFS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "XLF",
                        "XLE", "XLK", "XLV", "XLC", "TQQQ", "SQQQ", "UVXY"}
        _is_etf = ticker.upper() in _LIQUID_ETFS

        if _is_etf:
            _eff_max_spread  = 0.25
            _eff_min_oi      = 100
            _eff_min_volume  = 10
        else:
            _eff_max_spread  = self.max_spread_pct
            _eff_min_oi      = 25
            _eff_min_volume  = 0

        _INDEX_MAP = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM", "^DJI": None}
        if ticker.startswith("^"):
            mapped = _INDEX_MAP.get(ticker.upper())
            if mapped:
                log.info("[%s] Index ticker remapped to %s for options chain", ticker, mapped)
                ticker = mapped
            else:
                log.warning("[%s] Index ticker has no options mapping -- skipping", ticker)
                self._emit_selector_event(
                    plan,
                    stage="selector_entry",
                    decision="REJECT",
                    reason_code="UNSUPPORTED_INDEX_MAPPING",
                    explanation=f"Index ticker {ticker} has no supported options mapping",
                    inputs={"ticker": ticker},
                )
                return None

        pt1 = getattr(plan, "target_underlying", None)
        wick_targets = getattr(plan, "wick_targets", None) or []
        _expected_move_pct = 0.0
        _wick_confidence   = 0.5
        if wick_targets:
            _expected_move_pct = float(wick_targets[0].get("distance_pct", 0) or 0)
            _wick_confidence   = float(wick_targets[0].get("confidence", 0.5) or 0.5)
        elif pt1 and pt1 > 0:
            entry_approx = getattr(plan, "trigger_price", pt1) or pt1
            _expected_move_pct = abs(pt1 - entry_approx) / entry_approx * 100 if entry_approx else 0

        _plan_tier = _safe_plan_attr(plan, "tier", "B") or "B"
        log.info("[%s] ContractSelector | direction=%s budget=$%.0f tier=%s pt1=%s",
                 ticker, direction, budget, _plan_tier, pt1)

        # ── GATE 1: EARNINGS BLACKOUT ─────────────────────────────────────────
        if self.earnings_guard is not None:
            try:
                eg_result = self.earnings_guard.check(ticker)
                if eg_result.get("blocked"):
                    log.warning("[%s] BLOCKED by EarningsGuard -- %s",
                                ticker, eg_result.get("reason", "earnings blackout"))
                    self._emit_selector_event(
                        plan,
                stage="earnings_gate",
                        decision="REJECT",
                        reason_code="EARNINGS_LOCKOUT",
                        explanation=f"Blocked by EarningsGuard: {eg_result.get('reason','earnings blackout')}",
                        thresholds={"blackout_days": getattr(self.earnings_guard, "blackout_days", None)},
                    )
                    return None
            except Exception as exc:
                fail_open = bool(_SELECTOR_FAIL_OPEN_GUARD_ERRORS)
                log.warning(
                    "[%s] EarningsGuard raised unexpectedly (%s) -- %s",
                    ticker, exc, "continuing by override" if fail_open else "blocking fail-closed",
                )
                self._emit_selector_event(
                    plan,
                    stage="earnings_gate",
                    decision="ERROR" if fail_open else "REJECT",
                    reason_code="EARNINGS_GUARD_ERROR",
                    explanation=(
                        f"EarningsGuard exception; selector "
                        f"{'continued fail-open by env override' if fail_open else 'blocked fail-closed'}: {exc}"
                    ),
                    inputs={"ticker": ticker},
                    context={"fail_open": fail_open},
                )
                if not fail_open:
                    return None

        # ── PR1 (deferred-dte-ladder) ────────────────────────────────────────
        # Route eligible plans through the DTE-bucket ladder, but ONLY when:
        #   - the flag is enabled, AND
        #   - this is not already a ladder sub-call (expiration_override is None).
        #
        # AMENDMENT: the ladder gate is intentionally placed AFTER the terminal
        # non-DTE gates above (INVALID_PLAN, UNSUPPORTED_INDEX_MAPPING,
        # EARNINGS_LOCKOUT / EARNINGS_GUARD_ERROR). Those gates are ticker-level —
        # they give the same verdict regardless of expiration — so they must
        # short-circuit with their TRUE reason before the ladder runs. This
        # prevents the ladder from probing every DTE bucket and then overwriting
        # _last_failure with NO_VALID_PLAYBOOK_DTE_CONTRACT, which would mask a
        # real EARNINGS_LOCKOUT or invalid-plan rejection. Running them once here
        # is also cheaper than re-running them inside every ladder sub-call.
        #
        # A ladder sub-call passes an explicit expiration_override and falls
        # through to the normal single-expiration path below. When the flag is
        # off, expiration_override is always None and behavior is unchanged.
        if (
            self.dte_ladder_enabled
            and expiration_override is None
            and self._is_ladder_eligible(plan)
        ):
            return self._select_with_dte_ladder(plan)

        # ── A. FETCH CHAIN ────────────────────────────────────────────────────
        try:
            chain, underlying_price = self._fetch_chain_with_price(
                ticker, direction, expiration_override=expiration_override
            )
        except ChainAuthError as e:
            _expl = f"Chain provider auth failed ({getattr(e, 'status_code', '?')}): {e}"
            log.error("[%s] chain auth error (401/403): %s", ticker, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_AUTH_ERROR",
                explanation=_expl,
                inputs={"ticker": ticker, "direction": direction, "status_code": getattr(e, "status_code", None)},
            )
            _attach_selector_failure(plan, reason_code="CHAIN_AUTH_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None
        except ChainEmptyExpirations as e:
            _expl = f"Provider returned empty expirations list: {e}"
            log.warning("[%s] chain provider returned no expirations: %s", ticker, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
                explanation=_expl,
                inputs={"ticker": ticker},
            )
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_EMPTY_EXPIRATIONS", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None
        except NoExpirationInDTEWindow as e:
            _expl = f"No valid expiration within DTE window [{self.min_dte},{self.max_dte}]: {e}"
            log.warning("[%s] no expiration in DTE window [%d,%d]: %s", ticker, self.min_dte, self.max_dte, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="NO_EXPIRATION_IN_DTE_WINDOW",
                explanation=_expl,
                inputs={"ticker": ticker, "min_dte": self.min_dte, "max_dte": self.max_dte},
            )
            _attach_selector_failure(plan, reason_code="NO_EXPIRATION_IN_DTE_WINDOW", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None
        except ChainEmptyOptions as e:
            _expl = f"Provider returned zero option rows for this expiration: {e}"
            log.warning("[%s] chain provider returned empty options list: %s", ticker, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_PROVIDER_EMPTY_OPTIONS",
                explanation=_expl,
                inputs={"ticker": ticker, "direction": direction},
            )
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_EMPTY_OPTIONS", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None
        except ChainProviderError as e:
            _expl = f"Chain provider HTTP/network error: {e}"
            log.error("[%s] chain provider error (status=%s): %s", ticker, getattr(e, "status_code", "?"), e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_PROVIDER_ERROR",
                explanation=_expl,
                inputs={"ticker": ticker, "direction": direction, "status_code": getattr(e, "status_code", None)},
            )
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None
        except Exception as e:
            _expl = f"Chain fetch unexpected error: {e}"
            log.error("[%s] chain fetch unexpected error: %s", ticker, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_PROVIDER_ERROR",
                explanation=_expl,
                inputs={"ticker": ticker, "direction": direction},
            )
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode)
            return None

        if not chain:
            _ce_expl = (
                f"No {direction} options after direction filter "
                f"(chain rows may be all-opposite-direction or un-parseable)"
            )
            log.warning("[%s] CHAIN_PARSE_EMPTY -- no %s options after direction filter", ticker, direction)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="CHAIN_PARSE_EMPTY",
                explanation=_ce_expl,
                inputs={"ticker": ticker, "direction": direction},
            )
            _attach_selector_failure(
                plan,
                reason_code="CHAIN_PARSE_EMPTY",
                explanation=_ce_expl,
                chain_rows=0,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
            )
            return None
        _sel_chain_rows = len(chain)

        if not underlying_price:
            underlying_price = getattr(plan, "trigger_price", None)

        # ── GATE 2: IV RANK FILTER ────────────────────────────────────────────
        if self.iv_filter is not None:
            try:
                iv_result = self.iv_filter.check(
                    ticker, option_chain=chain,
                    underlying_price=underlying_price or 0.0,
                )
                if iv_result.get("blocked"):
                    log.warning("[%s] BLOCKED by IVRankFilter -- %s",
                                ticker, iv_result.get("reason", "IV rank too high"))
                    _sig_id = getattr(plan, "signal_id", "") or ""
                    trace_gate(str(_sig_id), ticker, "IV_GATE", "REJECT",
                               reason="iv_extreme", iv_rank=iv_result.get("iv_rank"))
                    self._emit_selector_event(
                        plan,
                stage="iv_gate",
                        decision="REJECT",
                        reason_code="IV_RANK_TOO_HIGH",
                        explanation=f"Blocked by IVRankFilter: {iv_result.get('reason','IV rank too high')}",
                        inputs={"iv_rank": iv_result.get("iv_rank"), "iv_zone": iv_result.get("iv_zone")},
                    )
                    return None

                if iv_result.get("requires_momentum"):
                    iv_zone      = iv_result.get("iv_zone", "soft")
                    signal_score = float(plan.score if hasattr(plan, "score") else 0)
                    min_score    = float(iv_result.get("momentum_min_score", 65.0))
                    mom_ok       = signal_score >= min_score
                    _sig_id      = getattr(plan, "signal_id", "") or ""
                    if not mom_ok:
                        log.warning("[%s] IVGate reject | iv_zone=%s score=%.1f need>=%.0f",
                                    ticker, iv_zone, signal_score, min_score)
                        trace_gate(str(_sig_id), ticker, "IV_GATE", "REJECT",
                                   reason=f"iv_zone={iv_zone}_score_too_low",
                                   score=signal_score, iv_rank=iv_result.get("iv_rank"))
                        self._emit_selector_event(
                            plan,
                stage="iv_gate",
                            decision="REJECT",
                            reason_code="IV_ZONE_SCORE_TOO_LOW",
                            explanation=(
                                f"IV zone {iv_zone} requires momentum score >= {min_score:.0f}, "
                                f"signal score={signal_score:.1f} is below threshold"
                            ),
                            inputs={
                                "iv_rank":   iv_result.get("iv_rank"),
                                "iv_zone":   iv_zone,
                                "score":     signal_score,
                                "min_score": min_score,
                            },
                            thresholds={"momentum_min_score": min_score},
                        )
                        return None
                    else:
                        log.info("[%s] IVGate allow | iv_zone=%s score=%.1f",
                                 ticker, iv_zone, signal_score)
                        trace_gate(str(_sig_id), ticker, "IV_GATE", "ALLOW",
                                   reason=f"iv_zone={iv_zone}",
                                   score=signal_score, iv_rank=iv_result.get("iv_rank"))
                        self._emit_selector_event(
                            plan,
                stage="iv_gate",
                            decision="ALLOW",
                            reason_code="IV_ZONE_OK",
                            explanation=f"IV zone {iv_zone} passed with momentum score {signal_score:.1f}",
                            inputs={
                                "iv_rank":   iv_result.get("iv_rank"),
                                "iv_zone":   iv_zone,
                                "score":     signal_score,
                                "min_score": min_score,
                            },
                            thresholds={"momentum_min_score": min_score},
                        )
            except Exception as exc:
                fail_open = bool(_SELECTOR_FAIL_OPEN_GUARD_ERRORS)
                log.warning(
                    "[%s] IVRankFilter raised unexpectedly (%s) -- %s",
                    ticker, exc, "continuing by override" if fail_open else "blocking fail-closed",
                )
                self._emit_selector_event(
                    plan,
                    stage="iv_gate",
                    decision="ERROR" if fail_open else "REJECT",
                    reason_code="IV_FILTER_ERROR",
                    explanation=(
                        f"IVRankFilter exception; selector "
                        f"{'continued fail-open by env override' if fail_open else 'blocked fail-closed'}: {exc}"
                    ),
                    inputs={"ticker": ticker, "underlying_price": _safe_float(underlying_price)},
                    context={"fail_open": fail_open},
                )
                if not fail_open:
                    return None

        # Fail closed on stub price data
        if isinstance(plan, dict):
            _intel = plan.get("intel_result") or {}
        else:
            _intel = getattr(plan, "intel_result", {}) or {}
        if _intel.get("price_data_stub"):
            log.warning("[%s] BLOCKED — price data was stub during intel gate", ticker)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="NO_CHAIN_DATA",
                explanation="Price data was a stub during intel gate — signal quality insufficient",
                inputs={"price_data_stub": True},
            )
            return None

        # P0 PR #302 Fix 4: build chain quote validity BEFORE filtering so we can
        # distinguish "all quotes were zero (data issue)" from "quotes were real
        # but failed OI/spread gates (quality issue)". Never blocks. Best-effort.
        _chain_quote_validity: dict = {}
        try:
            _chain_quote_validity = _build_chain_quote_validity(chain)
        except Exception:
            pass

        # P0 PR #302 Fix 1: detect effective paper selector data domain.
        _is_paper_mode = str(_sel_mode or "").lower() in ("paper",)
        _paper_sel_domain = _detect_paper_selector_domain(_sel_mode, _sel_base_url)
        _is_sandbox_data  = _paper_sel_domain == "sandbox"

        # P0 PR #302 Fix 2: track direct quote recovery at the select() level
        # (P0A per-row recovery is already in the quality filter loop).
        _direct_quote_recovery_audit: dict = {
            "attempted": False,
            "selected":  False,
            "contract":  None,
            "bid":       None,
            "ask":       None,
            "mid":       None,
            "failure":   None,
        }

        # ── B. HARD QUALITY FILTER ────────────────────────────────────────────
        # No self-mutation: effective thresholds passed directly to _quality_filter.
        # This is thread-safe when worker_loop and entry_watcher breach both call
        # select() on the same instance simultaneously.
        today      = date.today()
        survivors  = []
        _rejections: dict = {}
        _pro_tiers:  dict = {"A": 0, "B": 0}
        # P2: per-selector-pass direct-quote budget so one stale chain cannot
        # trigger hundreds of Tradier quote fetches.
        _p0a_budget: int = _DIRECT_QUOTE_RECOVERY_TOP_N

        for opt in chain:
            if _PRO_QUALITY_ENABLED:
                exp_str = opt.get("expiration_date", "")
                _dte = 0
                if exp_str:
                    try:
                        _dte = (date.fromisoformat(exp_str) - today).days
                    except Exception:
                        _dte = 0
                pro_tier, pro_reason = _pro_contract_quality(opt, ticker, _dte)

                # ── P0A/FIX-2: direct quote recovery inside pro-quality ───────
                # When pro_quality would hard-reject for a revalidatable reason
                # (zero/missing bid-ask, bid_below_0.1) AND the market is open
                # AND the per-pass budget allows it, fetch a direct option quote
                # and rerun pro_quality on the patched opt before rejecting.
                # _p0a_budget is checked before AND decremented after every call
                # regardless of the action result, so the cap applies to all
                # outcomes: PASS, REJECT_DIRECT_ZERO, REJECT_UNAVAILABLE, SKIP_*.
                if pro_tier == "REJECT" and _should_revalidate(pro_reason) and _p0a_budget > 0:
                    _rv_pro = _revalidate_direct(
                        self.data_broker,
                        opt,
                        pro_reason,
                    )
                    _p0a_budget -= 1   # always decrement — counts the fetch attempt
                    if _rv_pro.get("action") == "PASS" and _rv_pro.get("opt_updated"):
                        _opt_pro = _rv_pro["opt_updated"]
                        # Rerun pro_quality with patched bid/ask
                        pro_tier, pro_reason = _pro_contract_quality(_opt_pro, ticker, _dte)
                        if pro_tier != "REJECT":
                            # Pro quality passed on direct quote — use patched opt
                            opt = _opt_pro
                            log.info(
                                "[%s] P0A pro_quality_recovered chain_reason=%s "                                "direct_bid=%.4f direct_ask=%.4f contract=%s",
                                ticker, _rv_pro["audit"].get("chain_bid", 0),
                                _rv_pro["audit"].get("direct_bid", 0),
                                _rv_pro["audit"].get("direct_ask", 0),
                                opt.get("symbol", "?"),
                            )
                        # else: direct quote fetched but still fails — fall through to reject
                    elif _rv_pro.get("action") == "REJECT_DIRECT_ZERO":
                        # P1: name the specific failure — zero bid/ask on direct quote
                        pro_reason = "DIRECT_QUOTE_ZERO_BID_ASK"
                    elif _rv_pro.get("action") == "REJECT_UNAVAILABLE":
                        pro_reason = _rv_pro.get("reason_code") or "QUOTE_FETCH_FAILED"
                    # SKIP_NOT_MARKET_HOURS / SKIP_NOT_REVALIDATABLE: original reason stands.
                # ── end P0A/FIX-2 ────────────────────────────────────────────

                if pro_tier == "REJECT":
                    _rejections[pro_reason] = _rejections.get(pro_reason, 0) + 1
                    try:
                        self._emit_selector_event(
                            plan,
                            stage="quality_filter",
                            decision="REJECT",
                            reason_code=_normalize_reason_code(pro_reason),
                            explanation=pro_reason,
                            contract=opt.get("symbol"),
                            inputs={
                                "bid":        _safe_float(opt.get("bid")),
                                "ask":        _safe_float(opt.get("ask")),
                                "volume":     int(opt.get("volume") or 0),
                                "oi":         int(opt.get("open_interest") or 0),
                                "dte":        _dte,
                                "spread_pct": round(
                                    ((_safe_float(opt.get("ask")) - _safe_float(opt.get("bid")))
                                     / max((_safe_float(opt.get("ask")) + _safe_float(opt.get("bid"))) / 2, 0.01)),
                                    4
                                ),
                            },
                            thresholds={
                                "hard_spread": (
                                    _PRO_T1_SPREAD_HARD_MAX
                                    if (ticker.upper() in _PRO_TIER1_TICKERS)
                                    else _PRO_T2_SPREAD_HARD_MAX
                                ),
                                "min_bid": _PRO_MIN_BID,
                                "min_bid_size": _PRO_MIN_BID_SIZE_HARD,
                            },
                        )
                    except Exception:
                        pass  # per-contract pro-quality emit — non-critical
                    continue
                opt["_pro_tier"] = pro_tier
                _pro_tiers[pro_tier] = _pro_tiers.get(pro_tier, 0) + 1

            result = self._quality_filter(
                opt, today,
                max_spread_pct=_eff_max_spread,
                min_oi=_eff_min_oi,
                min_volume=_eff_min_volume,
            )

            # ── P0A: direct quote revalidation ────────────────────────────
            # When the chain row produced a revalidatable reject (zero/missing
            # bid-ask, bid_below_0.1, NO_CHAIN_DATA, zero liquidity) AND the
            # market is open AND the per-pass budget allows it, fetch a direct
            # option quote and re-run quality checks against the fresh quote.
            # Safety rules (spread, premium, affordability, capital) are
            # re-enforced against the direct quote — never bypassed.
            # _p0a_budget is checked before AND decremented after every call
            # regardless of result, so REJECT_DIRECT_ZERO / REJECT_UNAVAILABLE
            # consume the budget exactly as PASS does.
            if result is not None and _should_revalidate(result) and _p0a_budget > 0:
                _rv = _revalidate_direct(
                    self.data_broker,
                    opt,
                    result,
                )
                _p0a_budget -= 1   # always decrement — counts the fetch attempt
                _rv_action = _rv.get("action")
                if _rv_action == "PASS" and _rv.get("opt_updated"):
                    # Direct quote was valid.  Re-run quality filter on the
                    # patched opt (bid/ask replaced with direct-quote values).
                    _opt_patched = _rv["opt_updated"]
                    _result2 = self._quality_filter(
                        _opt_patched, today,
                        max_spread_pct=_eff_max_spread,
                        min_oi=_eff_min_oi,
                        min_volume=_eff_min_volume,
                    )
                    if _result2 is None:
                        # Direct quote rescued this contract — use patched opt
                        log.info(
                            "[%s] P0A direct_quote_recovered chain=%s direct_bid=%.4f "
                            "direct_ask=%.4f contract=%s",
                            ticker, result,
                            _rv["audit"].get("direct_bid", 0),
                            _rv["audit"].get("direct_ask", 0),
                            opt.get("symbol", "?"),
                        )
                        # P0 PR #302 Fix 2: stamp recovery audit at select() level
                        try:
                            _dq_bid = float(_rv["audit"].get("direct_bid") or 0)
                            _dq_ask = float(_rv["audit"].get("direct_ask") or 0)
                            _direct_quote_recovery_audit.update({
                                "attempted": True,
                                "selected":  True,
                                "contract":  str(opt.get("symbol") or ""),
                                "bid":       _dq_bid,
                                "ask":       _dq_ask,
                                "mid":       round((_dq_bid + _dq_ask) / 2, 4) if _dq_bid and _dq_ask else None,
                                "failure":   None,
                            })
                        except Exception:
                            pass
                        survivors.append(_opt_patched)
                        # Emit PASS event with audit fields
                        try:
                            self._emit_selector_event(
                                plan,
                                stage="quality_filter_direct_quote",
                                decision="PASS",
                                reason_code="DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
                                explanation=(
                                    f"chain rejected ({result}) but direct "
                                    f"quote recovered bid={_rv['audit'].get('direct_bid')} "
                                    f"ask={_rv['audit'].get('direct_ask')}"
                                ),
                                contract=opt.get("symbol"),
                                context=_rv.get("audit") or {},
                            )
                        except Exception:
                            pass
                        continue   # ← skip the standard reject branch below
                    else:
                        # Direct quote fetched but still fails quality (e.g.
                        # spread too wide at direct prices) — reject with the
                        # real reason from the re-run, not the chain reason.
                        result = _result2
                elif _rv_action == "REJECT_DIRECT_ZERO":
                    # P1: direct quote returned zero bid/ask — use truthful reason code
                    result = "DIRECT_QUOTE_ZERO_BID_ASK"
                    # P0 PR #302 Fix 2: track failed recovery attempt
                    try:
                        if not _direct_quote_recovery_audit.get("selected"):
                            _direct_quote_recovery_audit.update({
                                "attempted": True,
                                "selected":  False,
                                "failure":   "DIRECT_QUOTE_ZERO_BID_ASK",
                            })
                    except Exception:
                        pass
                elif _rv_action == "REJECT_UNAVAILABLE":
                    # P1: quote source unavailable (network/auth failure)
                    result = _rv.get("reason_code") or "QUOTE_FETCH_FAILED"
                # SKIP_NOT_MARKET_HOURS / SKIP_NOT_REVALIDATABLE:
                # fall through with original chain reject reason unchanged.
            # ── end P0A ───────────────────────────────────────────────────

            if result is None:
                survivors.append(opt)
            else:
                _rejections[result] = _rejections.get(result, 0) + 1
                log.debug("[%s] filtered: %s -- %s", ticker, opt.get("symbol", "?"), result)
                # P0 (PR #299): track the best rejected candidate — the one with
                # the highest mid (i.e. most liquid/real) that still got rejected.
                # Persisted in the selector_failure audit so operators can answer
                # "was there ANY real contract near the money?" without scanning logs.
                try:
                    _opt_bid = _safe_float(opt.get("bid") or 0)
                    _opt_ask = _safe_float(opt.get("ask") or 0)
                    _opt_mid = (_opt_bid + _opt_ask) / 2.0 if _opt_bid and _opt_ask else 0.0
                    _opt_oi  = int(opt.get("open_interest") or 0)
                    _opt_vol = int(opt.get("volume") or 0)
                    _opt_sprd = (
                        round((_opt_ask - _opt_bid) / _opt_ask, 4)
                        if _opt_ask > 0 else None
                    )
                    _cur_best_mid = _safe_float(
                        (_best_rejected_candidate or {}).get("mid") or 0
                    )
                    if _opt_mid > _cur_best_mid:
                        _best_rejected_candidate = {
                            "symbol":        opt.get("symbol"),
                            "bid":           _opt_bid,
                            "ask":           _opt_ask,
                            "mid":           _opt_mid,
                            "ask_cost":      round(_opt_ask * 100, 2),
                            "open_interest": _opt_oi,
                            "volume":        _opt_vol,
                            "spread_pct":    _opt_sprd,
                            "rejection_reason": _normalize_reason_code(result),
                        }
                except Exception:
                    pass
                try:
                    self._emit_selector_event(
                        plan,
                        stage="quality_filter",
                        decision="REJECT",
                        reason_code=_normalize_reason_code(result),
                        explanation=result,
                        contract=opt.get("symbol"),
                        inputs={
                            "bid":     _safe_float(opt.get("bid")),
                            "ask":     _safe_float(opt.get("ask")),
                            "volume":  int(opt.get("volume") or 0),
                            "oi":      int(opt.get("open_interest") or 0),
                            "premium": round(
                                        ((_safe_float(opt.get("bid")) + _safe_float(opt.get("ask"))) / 2) * 100,
                                        2
                                    ) if opt.get("bid") is not None and opt.get("ask") is not None else 0,
                        },
                        thresholds={
                            "max_spread_pct": _safe_float(_eff_max_spread),
                            "min_oi":         _safe_float(_eff_min_oi),
                            "min_premium":    self.min_premium,
                            "max_premium":    self.max_premium,
                        },
                    )
                except Exception:
                    pass  # per-contract quality-filter emit — non-critical

        _sel_survivors   = len(survivors)
        _sel_rejections  = dict(_rejections)  # snapshot for selector_failure

        if not survivors:
            log.warning(
                "[%s] CONTRACT_SELECTOR: NO_ELIGIBLE_CONTRACTS | chain=%d | rejections: %s",
                ticker, len(chain),
                ", ".join(f"{k}({v})" for k, v in
                          sorted(_rejections.items(), key=lambda x: -x[1]))
                if _rejections else "none",
            )
            _top_reject = (
                sorted(_rejections.items(), key=lambda x: -x[1])[0][0]
                if _rejections else None
            )
            _obs_reason = _normalize_reason_code(_top_reject) if _top_reject else "NO_CONTRACT_AFTER_FILTERS"
            self._emit_selector_event(
                plan,
                stage="quality_summary",
                decision="REJECT",
                reason_code=_obs_reason,
                explanation=(
                    "No contracts passed quality gates | chain=" + str(len(chain)) + " | "
                    "top_reason=" + (_top_reject or "none") + " | "
                    + (", ".join(f"{k}({v})" for k, v in
                       sorted(_rejections.items(), key=lambda x: -x[1]))
                       if _rejections else "none")
                ),
                inputs={
                    "chain_size":       len(chain),
                    "rejection_counts": {_normalize_reason_code(k): v for k, v in _rejections.items()},
                    "raw_rejection_counts": _rejections,
                    # PR2: budget context so the operator can correlate a
                    # whole-chain quality wipeout with a too-small account
                    # (e.g. only deep-OTM junk was affordable). Diagnostic only.
                    "budget":                 _safe_float(budget),
                    "max_affordable_premium": _safe_float(_safe_plan_attr(plan, "max_affordable_premium", 0)) or None,
                    "underlying_price":       _safe_float(underlying_price) or None,
                },
                thresholds={
                    "max_spread_pct": _safe_float(_eff_max_spread),
                    "min_oi":         _safe_float(_eff_min_oi),
                },
                context={
                    "candidate_table": _build_candidate_audit(
                        [],
                        underlying_price or 0.0,
                        selected_symbol=None,
                        selected_reason="no_survivors",
                        rejections=_rejections,
                        top_n=int(os.getenv("SELECTOR_CANDIDATE_TABLE_TOP_N", "15")),
                    ),
                },
            )
            # P0 PR #302 Fix 1: paper + sandbox data + zero-quote failure →
            # classify as PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE so operators
            # can distinguish data-domain issues from contract-quality issues.
            _final_reason = _obs_reason
            try:
                if (_is_paper_mode and _is_sandbox_data
                        and _is_paper_sandbox_data_failure(_obs_reason)):
                    _final_reason = "PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE"
                    log.warning(
                        "[%s] PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE "
                        "original_reason=%s base_url=%s — "
                        "paper selector is using sandbox Tradier data which "
                        "has 15-min delayed/zero quotes. "
                        "Set TRADIER_MARKET_DATA_TOKEN to use live data.",
                        ticker, _obs_reason, _sel_base_url,
                    )
            except Exception:
                pass
            _attach_selector_failure(
                plan,
                reason_code=_final_reason,
                explanation=(
                    "No contracts passed quality gates"
                    f" | chain={_sel_chain_rows}"
                    f" | top_reason={_top_reject or 'none'}"
                    + (f" | paper_domain={_paper_sel_domain}" if _is_paper_mode else "")
                ),
                chain_rows=_sel_chain_rows,
                survivor_count=0,
                reject_buckets=_sel_rejections,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                best_rejected_candidate=_best_rejected_candidate,
                chain_quote_validity=_chain_quote_validity or None,      # Fix 4
                direct_quote_recovery_audit=_direct_quote_recovery_audit, # Fix 2
            )
            return None

        if _PRO_QUALITY_ENABLED:
            log.info("[%s] %d contracts passed pro quality | A=%d B=%d",
                     ticker, len(survivors), _pro_tiers.get("A", 0), _pro_tiers.get("B", 0))
        else:
            log.info("[%s] %d contracts passed quality filter", ticker, len(survivors))

        # ── C. RANK ───────────────────────────────────────────────────────────
        scored = []
        for opt in survivors:
            s = self._rank_score(
                opt, budget,
                expected_move_pct=_expected_move_pct,
                underlying_price=underlying_price or 0.0,
                tier=getattr(plan, "tier", "B") or "B",
            )
            scored.append((s, opt))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best = scored[0]

        # ── CANDIDATE AUDIT — log why every contract won or lost ──────────────
        # This is how you answer "why did we pick $0.11 instead of $0.50?"
        try:
            _audit_top = scored[:int(os.getenv("CONTRACT_CANDIDATE_AUDIT_TOP_N", "10"))]
            for _rank, (_s, _c) in enumerate(_audit_top):
                _ab, _ar = _extract_abs_delta(_c)
                _bid = _safe_float(_c.get("bid"))
                _ask = _safe_float(_c.get("ask"))
                _prem = ((_bid + _ask) / 2) * 100
                log.info(
                    "[%s] CANDIDATE #%d | %s | $%.2f prem=$%.0f delta=%s score=%.1f",
                    ticker, _rank + 1,
                    _c.get("symbol", "?"),
                    (_bid + _ask) / 2,
                    _prem,
                    f"{_ab:.3f}" if _ab is not None else f"MISSING({_ar})",
                    _s,
                )
        except Exception:
            pass

        # ── CHEAP CONTRACT UPGRADE PASS ───────────────────────────────────────
        # If selected contract is below MIN_ACCEPTABLE_PREMIUM, look for a
        # better-priced contract in the $50–$350 range.
        # Cheap contracts ($0.10–$0.49) are fragile: 1 cent move = -10% loss,
        # fills are hard, and exits fail repeatedly (as seen with NVDA $0.11).
        _MIN_ACCEPTABLE_PREMIUM = float(os.getenv("MIN_ACCEPTABLE_PREMIUM_PER_CONTRACT", "50"))
        _MAX_UPGRADE_PREMIUM    = float(os.getenv("MAX_IDEAL_PREMIUM_PER_CONTRACT", "350"))

        is_live_upgrade, _ = _pricing_basis_for_mode(getattr(self, "mode", "paper"))

        def _exec_premium(opt: dict) -> float:
            b = _safe_float(opt.get("bid"))
            a = _safe_float(opt.get("ask"))
            ep = a if is_live_upgrade else (b + a) / 2
            return ep * 100

        best_exec_prem = _exec_premium(best)

        if best_exec_prem < _MIN_ACCEPTABLE_PREMIUM:
            upgrade_pool = []
            for _s, _c in scored[1:]:  # already sorted best-first
                _prem = _exec_premium(_c)
                if _MIN_ACCEPTABLE_PREMIUM <= _prem <= _MAX_UPGRADE_PREMIUM:
                    _delta_val, _ = _extract_abs_delta(_c)
                    if _delta_val is not None:  # require real Greeks on upgrade
                        upgrade_pool.append((_s, _c, _prem))

            if upgrade_pool:
                upgrade_pool.sort(key=lambda x: x[0], reverse=True)
                old_sym   = best.get("symbol")
                old_prem  = best_exec_prem
                best_score, best, new_prem = upgrade_pool[0]
                log.warning(
                    "[%s] CONTRACT_UPGRADE | %s ($%.0f) → %s ($%.0f) | reason=cheap_contract_upgrade",
                    ticker, old_sym, old_prem, best.get("symbol"), new_prem,
                )
                self._emit_selector_event(
                    plan, stage="contract_upgrade", decision="ALLOW",
                    reason_code="CHEAP_CONTRACT_UPGRADED",
                    explanation=(
                        f"Selected {old_sym} premium ${old_prem:.0f} below minimum "
                        f"${_MIN_ACCEPTABLE_PREMIUM:.0f}; upgraded to "
                        f"{best.get('symbol')} premium ${new_prem:.0f}"
                    ),
                    contract=best.get("symbol"),
                    inputs={"old_contract": old_sym, "old_premium": round(old_prem, 2),
                            "new_contract": best.get("symbol"), "new_premium": round(new_prem, 2)},
                    thresholds={"min_acceptable_premium": _MIN_ACCEPTABLE_PREMIUM,
                                "max_upgrade_premium": _MAX_UPGRADE_PREMIUM},
                )
            else:
                _allow_cheap = os.getenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "false").lower() == "true"
                log.warning(
                    "[%s] CHEAP_CONTRACT_%s | %s premium=$%.0f below min=$%.0f — no upgrade found",
                    ticker,
                    "ALLOWED" if _allow_cheap else "BLOCKED",
                    best.get("symbol"), best_exec_prem, _MIN_ACCEPTABLE_PREMIUM,
                )
                self._emit_selector_event(
                    plan, stage="cheap_contract_gate",
                    decision="ALLOW" if _allow_cheap else "REJECT",
                    reason_code="CHEAP_CONTRACT_NO_UPGRADE" if not _allow_cheap else "CHEAP_CONTRACT_ONLY_CHOICE",
                    explanation=(
                        f"{best.get('symbol')} premium ${best_exec_prem:.0f} below "
                        f"${_MIN_ACCEPTABLE_PREMIUM:.0f} and no upgrade available. "
                        f"{'Allowed by ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE.' if _allow_cheap else 'Blocked.'}"
                    ),
                    contract=best.get("symbol"),
                    inputs={"selected_contract": best.get("symbol"),
                            "selected_premium": round(best_exec_prem, 2)},
                    thresholds={"min_acceptable_premium": _MIN_ACCEPTABLE_PREMIUM,
                                "allow_cheap_if_only_choice": _allow_cheap},
                    context={
                        "candidate_table": _build_candidate_audit(
                            scored,
                            underlying_price or 0.0,
                            selected_symbol=best.get("symbol"),
                            selected_reason="cheap_contract_no_upgrade",
                            rejections=_rejections,
                            top_n=int(os.getenv("SELECTOR_CANDIDATE_TABLE_TOP_N", "15")),
                        ),
                    },
                )
                if not _allow_cheap:
                    _attach_selector_failure(
                        plan,
                        reason_code="NO_AFFORDABLE_CONTRACT",
                        explanation=(
                            f"{best.get('symbol','?')} premium ${best_exec_prem:.0f}"
                            f" below min ${_MIN_ACCEPTABLE_PREMIUM:.0f}"
                            f" and no upgrade found | chain={_sel_chain_rows}"
                            f" survivors={_sel_survivors}"
                        ),
                        chain_rows=_sel_chain_rows,
                        survivor_count=_sel_survivors,
                        reject_buckets=_sel_rejections,
                        base_url=_sel_base_url,
                        execution_mode=_sel_mode,
                    )
                    return None
        selected = self._build_selected(best, best_score, budget, today)
        if selected is None:
            return None

        # Persist compact candidate-table evidence from the final sorted
        # survivor list + rejection counts. This is observational only.
        try:
            selected.candidate_audit = _build_candidate_audit(
                scored,
                underlying_price or 0.0,
                selected_symbol=selected.contract_symbol,
                selected_reason=selected.selection_reason,
                rejections=_rejections,
                top_n=int(os.getenv("SELECTOR_CANDIDATE_TABLE_TOP_N", "15")),
            )
        except Exception:
            selected.candidate_audit = None
        _candidate_context = {"candidate_table": selected.candidate_audit} if selected.candidate_audit else None

        _effective_budget_used, _max_trade_cap, _budget_was_clipped = _effective_budget(budget)
        if _budget_was_clipped:
            log.warning(
                "[%s] Budget clipped by selector | upstream_budget=$%.0f max_trade_usd=$%.0f effective_budget=$%.0f",
                ticker, budget, _max_trade_cap, _effective_budget_used,
            )
            self._emit_selector_event(
                plan,
                stage="budget_gate",
                decision="ALLOW",
                reason_code="BUDGET_CLIPPED_BY_SELECTOR",
                explanation=(
                    f"Upstream budget ${budget:.0f} clipped to MAX_TRADE_USD "
                    f"${_max_trade_cap:.0f}; effective selector budget=${_effective_budget_used:.0f}"
                ),
                contract=selected.contract_symbol,
                inputs={
                    "upstream_budget": _safe_float(budget),
                    "effective_budget": _safe_float(_effective_budget_used),
                    "max_trade_usd": _safe_float(_max_trade_cap),
                    "premium_per_contract": _safe_float(selected.premium_per_contract),
                },
                thresholds={"max_trade_usd": _max_trade_cap},
                context={"budget_clipped": True},
            )

        # ── E. AFFORDABILITY GATE ────────────────────────────────────────────
        if selected.affordable_contracts < 1:
            if self.mode.upper() != "LIVE":
                if selected.premium_per_contract > self.max_premium:
                    log.warning("[%s] premium $%.0f > max $%.0f -- skipping (too expensive for paper)",
                                ticker, selected.premium_per_contract, self.max_premium)
                    self._emit_selector_event(
                        plan,
                stage="affordability_gate",
                        decision="REJECT",
                        reason_code="PAPER_PREMIUM_CAP",
                        explanation=(
                            f"Paper premium cap: ${selected.premium_per_contract:.0f} "
                            f"> max ${self.max_premium:.0f} — valid contract but too expensive for paper."
                        ),
                        contract=selected.contract_symbol,
                        inputs={
                            "premium_per_contract": _safe_float(selected.premium_per_contract),
                            "max_premium":          _safe_float(self.max_premium),
                        },
                        thresholds={"max_premium": self.max_premium},
                        context=dict({"mode": self.mode}, **(_candidate_context or {})),
                    )
                    _attach_selector_failure(
                        plan,
                        reason_code="PREMIUM_CAP_EXCEEDED",
                        explanation=(
                            f"Paper premium cap: ${selected.premium_per_contract:.0f}"
                            f" > max ${self.max_premium:.0f}"
                        ),
                        chain_rows=_sel_chain_rows,
                        survivor_count=_sel_survivors,
                        reject_buckets=_sel_rejections,
                        base_url=_sel_base_url,
                        execution_mode=_sel_mode,
                    )
                    return None
                log.warning("[%s] budget $%.0f < premium $%.0f -- forcing 1 contract",
                            ticker, budget, selected.premium_per_contract)
                self._emit_selector_event(
                    plan,
                stage="affordability_gate",
                    decision="ALLOW",
                    reason_code="FORCED_1_FOR_PAPER",
                    explanation=(
                        f"Paper override: budget ${budget:.0f} < premium "
                        f"${selected.premium_per_contract:.0f} — forcing 1 contract. "
                        "NOT valid for live trading."
                    ),
                    contract=selected.contract_symbol,
                    inputs={
                        "budget":               _safe_float(budget),
                        "premium_per_contract": _safe_float(selected.premium_per_contract),
                    },
                    context={"mode": self.mode, "simulation_override": True},
                )
                selected = SelectedContract(
                    contract_symbol      = selected.contract_symbol,
                    expiration           = selected.expiration,
                    strike               = selected.strike,
                    option_type          = selected.option_type,
                    bid                  = selected.bid,
                    ask                  = selected.ask,
                    mid                  = selected.mid,
                    spread_pct           = selected.spread_pct,
                    delta                = selected.delta,
                    open_interest        = selected.open_interest,
                    volume               = selected.volume,
                    premium_per_share    = selected.premium_per_share,
                    premium_per_contract = selected.premium_per_contract,
                    affordable_contracts = 1,
                    scoring_price_per_share   = selected.scoring_price_per_share,
                    execution_price_per_share = selected.execution_price_per_share,
                    effective_budget          = selected.effective_budget,
                    budget_clipped            = selected.budget_clipped,
                    pricing_basis             = selected.pricing_basis,
                    selection_reason     = selected.selection_reason + " [forced_1]",
                    selection_score      = selected.selection_score,
                    dte                  = selected.dte,
                    candidate_audit      = selected.candidate_audit,
                )
            else:
                # ── PR2: account-size tradeability classification ─────────────
                # A QUALITY contract was found (it passed every liquidity / spread
                # / OI / delta gate) but it exceeds the per-trade budget. This is
                # not a data or liquidity failure — it is structurally untradeable
                # for THIS account size. Classify it precisely so the operator can
                # tell "account too small for this name right now" apart from
                # "bad data / no liquidity / wrong DTE". Breach-time authoritative:
                # this runs only after the chain (and, under PR1, the DTE ladder)
                # has been fully evaluated, so the quality contract is real.
                #
                # NOTE: this does NOT loosen any gate and does NOT change the
                # decision — the live reject still returns None. It only upgrades
                # the REASON from generic NO_AFFORDABLE_CONTRACT to the specific
                # UNTRADEABLE_FOR_ACCOUNT_SIZE, with full budget diagnostics.
                # Read sizing diagnostics from plan.metadata['sizing_context']
                # first (their canonical production location); fall back to
                # top-level attrs only if absent. Reading top-level alone returned
                # None in production, defeating the diagnostics.
                _equity = _safe_float(_sizing_val(plan, "account_equity", "equity", default=0)) or None
                _max_pos_pct = _safe_float(_sizing_val(plan, "risk_pct", "max_position_pct", default=0)) or None
                _max_afford_prem = _safe_float(_sizing_val(plan, "max_affordable_premium", default=0)) or None
                _ctx_budget = _safe_float(_sizing_val(plan, "budget", "max_position_usd", default=0)) or _safe_float(budget)
                _underlying = _safe_float(underlying_price) or None
                _tradeability_diag = {
                    "equity":                 _equity,
                    "max_position_pct":       _max_pos_pct,
                    "max_trade_usd":          _safe_float(_max_trade_usd()),
                    "max_affordable_premium": _max_afford_prem,
                    "underlying_price":       _underlying,
                    "budget":                 _ctx_budget,
                    "selected_dte":           _safe_float(getattr(selected, "dte", None)),
                    "near_atm_premium_estimate": _safe_float(selected.premium_per_contract),
                    "cheapest_quality_survivor_premium": _safe_float(selected.premium_per_contract),
                    "classification":         "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                    # P0 (PR #299): explicit cap-math fields so operators can prove
                    # exactly why the contract failed the account-size check. Previously
                    # this required mental math from the log line.
                    "ask_cost":              round(_safe_float(selected.premium_per_contract) * 100, 2),
                    "qty_attempted":         1,   # deferred breach always starts at qty=1 for materialization
                    "projected_reserved_cost": round(_safe_float(selected.premium_per_contract) * 100, 2),
                }
                log.warning(
                    "[%s] UNTRADEABLE_FOR_ACCOUNT_SIZE -- quality contract %s @ "
                    "$%.0f/contract exceeds budget $%.0f (equity=%s pct=%s) — "
                    "not a data/liquidity failure",
                    ticker, selected.contract_symbol,
                    selected.premium_per_contract, _ctx_budget, _equity, _max_pos_pct,
                )
                # Flatten the key diagnostics INTO the explanation string so they
                # survive downstream: ap/queue.py _derive_last_error and the
                # deferred-breach audit persist reason_code/stage/explanation but
                # may drop the structured tradeability_diag dict. Embedding the
                # numbers in explanation guarantees they reach the order row.
                _diag_summary = (
                    f"equity=${_equity or 0:.0f} budget=${_ctx_budget or 0:.0f} "
                    f"premium=${selected.premium_per_contract:.0f} "
                    f"underlying=${_underlying or 0:.2f} dte={getattr(selected,'dte',None)}"
                )
                _explanation = (
                    f"Quality contract {selected.contract_symbol} @ "
                    f"${selected.premium_per_contract:.0f}/contract exceeds budget "
                    f"${_ctx_budget or 0:.0f} — structurally untradeable for this account "
                    f"size [{_diag_summary}]. No ticker blacklist; eligible again if a "
                    f"cheaper quality contract appears or the account grows."
                )
                self._emit_selector_event(
                    plan,
                    stage="affordability_gate",
                    decision="REJECT",
                    reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE",
                    explanation=_explanation,
                    contract=selected.contract_symbol,
                    inputs=_tradeability_diag,
                    thresholds={"min_contracts": 1},
                    context=_candidate_context,
                )
                # Record as the authoritative last-failure reason for the queue
                # and the deferred-breach audit (consumed by PR3's taxonomy). Both
                # the structured diag AND the flattened explanation are included so
                # the numbers survive whichever downstream copies them.
                self._last_failure = {
                    "stage": "affordability_gate",
                    "reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                    "explanation": _explanation,
                    "tradeability_diag": _tradeability_diag,
                }
                _attach_selector_failure(
                    plan,
                    reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE",
                    explanation=_explanation,
                    chain_rows=_sel_chain_rows,
                    survivor_count=_sel_survivors,
                    reject_buckets=_sel_rejections,
                    base_url=_sel_base_url,
                    execution_mode=_sel_mode,
                    best_rejected_candidate=_best_rejected_candidate,
                )
                return None

        if selected is None:
            return None

        # ── DEEP OTM GATE ────────────────────────────────────────────────────
        # Reject contracts where delta is too low (too far OTM).
        # A delta below 0.10 means the option barely moves with the stock.
        # META $610 put when stock at $672 = 9% OTM, delta ~0.05 = lottery ticket.
        # Better to skip the trade than buy a contract that can't win.
        _MIN_DELTA = float(os.getenv("MIN_CONTRACT_DELTA", "0.10"))
        if selected.delta is not None and selected.delta < _MIN_DELTA:
            log.warning(
                "[%s] DEEP OTM GATE: delta=%.2f < min=%.2f — contract too far OTM, skipping",
                ticker, selected.delta, _MIN_DELTA,
            )
            self._emit_selector_event(
                plan,
                stage="deep_otm_gate",
                decision="REJECT",
                reason_code="DELTA_OUT_OF_RANGE",
                explanation=f"Deep OTM gate: delta={selected.delta:.2f} < min={_MIN_DELTA:.2f}",
                contract=selected.contract_symbol,
                inputs={"delta": selected.delta, "strike": selected.strike},
                thresholds={"min_delta": _MIN_DELTA},
                context=_candidate_context,
            )
            _attach_selector_failure(
                plan,
                reason_code="DELTA_OUT_OF_RANGE",
                explanation=(
                    f"Deep OTM: delta={selected.delta:.2f} < min={_MIN_DELTA:.2f}"
                    f" | {selected.contract_symbol}"
                ),
                chain_rows=_sel_chain_rows,
                survivor_count=_sel_survivors,
                reject_buckets=_sel_rejections,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                best_rejected_candidate=_best_rejected_candidate,
            )
            return None

        # Also check moneyness if underlying price available
        # Block if strike is more than 12% away from current price
        _MAX_OTM_PCT = float(os.getenv("MAX_OTM_PCT", "0.12"))
        if underlying_price and underlying_price > 0 and selected.strike > 0:
            _otm_pct = abs(selected.strike - underlying_price) / underlying_price
            if _otm_pct > _MAX_OTM_PCT:
                log.warning(
                    "[%s] DEEP OTM GATE: strike=%.2f underlying=%.2f OTM=%.1f%% > max=%.0f%% — skipping",
                    ticker, selected.strike, underlying_price,
                    _otm_pct * 100, _MAX_OTM_PCT * 100,
                )
                self._emit_selector_event(
                    plan,
                    stage="deep_otm_gate",
                    decision="REJECT",
                    reason_code="DELTA_OUT_OF_RANGE",
                    explanation=(
                        f"Deep OTM gate: strike={selected.strike:.2f} "
                        f"underlying={underlying_price:.2f} "
                        f"OTM={_otm_pct*100:.1f}% > max={_MAX_OTM_PCT*100:.0f}%"
                    ),
                    contract=selected.contract_symbol,
                    inputs={
                        "strike":           _safe_float(selected.strike),
                        "underlying_price": _safe_float(underlying_price),
                        "otm_pct":          round(_otm_pct, 4),
                        "delta":            _safe_float(selected.delta),
                    },
                    thresholds={"max_otm_pct": _MAX_OTM_PCT},
                    context=_candidate_context,
                )
                _attach_selector_failure(
                    plan,
                    reason_code="DELTA_OUT_OF_RANGE",
                    explanation=(
                        f"Deep OTM moneyness: strike={selected.strike:.2f}"
                        f" underlying={underlying_price:.2f}"
                        f" OTM={_otm_pct*100:.1f}% > max={_MAX_OTM_PCT*100:.0f}%"
                    ),
                    chain_rows=_sel_chain_rows,
                    survivor_count=_sel_survivors,
                    reject_buckets=_sel_rejections,
                    base_url=_sel_base_url,
                    execution_mode=_sel_mode,
                    best_rejected_candidate=_best_rejected_candidate,
                )
                return None

        # ── FINAL PREMIUM GATE (per-ticker cap) ──────────────────────────────
        _final_prem_cap = _get_max_premium(ticker)
        if selected.premium_per_contract > _final_prem_cap:
            log.warning("[%s] FINAL PREMIUM GATE: $%.0f > $%.0f per-ticker cap — blocking",
                        ticker, selected.premium_per_contract, _final_prem_cap)
            self._emit_selector_event(
                plan,
                stage="premium_gate",
                decision="REJECT",
                reason_code="PREMIUM_CAP_EXCEEDED",
                explanation=(
                    f"Final premium gate: ${selected.premium_per_contract:.0f} "
                    f"> ${_final_prem_cap:.0f} per-ticker cap for {ticker}"
                ),
                contract=selected.contract_symbol,
                inputs={
                    "premium_per_contract": _safe_float(selected.premium_per_contract),
                    "ticker_cap":           _safe_float(_final_prem_cap),
                },
                thresholds={"ticker_premium_cap": _final_prem_cap},
                context=_candidate_context,
            )
            _attach_selector_failure(
                plan,
                reason_code="PREMIUM_CAP_EXCEEDED",
                explanation=(
                    f"Final premium gate: ${selected.premium_per_contract:.0f}"
                    f" > ${_final_prem_cap:.0f} per-ticker cap for {ticker}"
                ),
                chain_rows=_sel_chain_rows,
                survivor_count=_sel_survivors,
                reject_buckets=_sel_rejections,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                best_rejected_candidate=_best_rejected_candidate,
            )
            return None

        # ── F. UPDATE PLAN IN-PLACE ───────────────────────────────────────────
        # Default behavior remains mutation because downstream execution expects
        # the selected contract fields on the plan. Set mutate_plan=False only
        # for audit/replay callers that explicitly consume SelectedContract.
        if self.mutate_plan:
            if isinstance(plan, dict):
                plan["contract_symbol"] = selected.contract_symbol
                plan["limit_price"]     = selected.execution_price_per_share
                plan["contracts"]       = selected.affordable_contracts
                plan["max_position_usd"] = plan["contracts"] * selected.premium_per_contract
                plan["selector_effective_budget"] = selected.effective_budget
                plan["selector_budget_clipped"] = selected.budget_clipped
                plan["selector_scoring_price"] = selected.scoring_price_per_share
                plan["selector_execution_price"] = selected.execution_price_per_share
                plan["selector_pricing_basis"] = selected.pricing_basis
                plan.setdefault("selector_metadata", {})
                plan["selector_metadata"].update({
                    "contract_symbol": selected.contract_symbol,
                    "pricing_basis": selected.pricing_basis,
                    "premium_per_contract": selected.premium_per_contract,
                    "affordable_contracts": selected.affordable_contracts,
                    "effective_budget": selected.effective_budget,
                    "budget_clipped": selected.budget_clipped,
                })
            else:
                plan.contract_symbol  = selected.contract_symbol
                plan.limit_price      = selected.execution_price_per_share
                plan.contracts        = selected.affordable_contracts
                plan.max_position_usd = plan.contracts * selected.premium_per_contract
                plan.selector_effective_budget = selected.effective_budget
                plan.selector_budget_clipped = selected.budget_clipped
                plan.selector_scoring_price = selected.scoring_price_per_share
                plan.selector_execution_price = selected.execution_price_per_share
                plan.selector_pricing_basis = selected.pricing_basis
                try:
                    meta = getattr(plan, "selector_metadata", None) or {}
                    meta.update({
                        "contract_symbol": selected.contract_symbol,
                        "pricing_basis": selected.pricing_basis,
                        "premium_per_contract": selected.premium_per_contract,
                        "affordable_contracts": selected.affordable_contracts,
                        "effective_budget": selected.effective_budget,
                        "budget_clipped": selected.budget_clipped,
                    })
                    plan.selector_metadata = meta
                except Exception:
                    pass
        else:
            log.info(
                "[%s] Selector mutate_plan=False — returning SelectedContract without mutating plan | %s",
                ticker, selected.contract_symbol,
            )

        if self.mutate_plan and _wick_confidence and not _safe_plan_attr(plan, "wick_confidence", None):
            try:
                if isinstance(plan, dict):
                    plan["wick_confidence"] = _wick_confidence
                else:
                    plan.wick_confidence = _wick_confidence
            except Exception:
                pass

        self._emit_selector_event(
            plan,
                stage="contract_selected",
            decision="ALLOW",
            reason_code="SELECTED",
            explanation=(
                f"Contract selected | score={selected.selection_score:.2f} | "
                f"{selected.selection_reason}"
            ),
            contract=selected.contract_symbol,
            inputs={
                "selection_score":      selected.selection_score,
                "premium_per_contract": selected.premium_per_contract,
                "scoring_price_per_share": selected.scoring_price_per_share,
                "execution_price_per_share": selected.execution_price_per_share,
                "effective_budget": selected.effective_budget,
                "budget_clipped": selected.budget_clipped,
                "pricing_basis": selected.pricing_basis,
                "spread_pct":           selected.spread_pct,
                "oi":                   selected.open_interest,
                "volume":               selected.volume,
                "delta":                selected.delta,
                "dte":                  selected.dte,
                "affordable_contracts": selected.affordable_contracts,
            },
            thresholds={
                "max_spread_pct": _eff_max_spread,
                "min_oi":         _eff_min_oi,
                "budget":         budget,
            },
            context={
                "is_etf":              _is_etf,
                "selection_reason":    selected.selection_reason,
                "mode":                self.mode,
                "pricing_basis":       selected.pricing_basis,
                "simulation_override": "[forced_1]" in (selected.selection_reason or ""),
                "quality_rules_version": _QUALITY_RULES_VERSION,
                "budget_clipped":       _budget_was_clipped,
            },
        )

        log.info(
            "[%s] SELECTED | %s bid=%s ask=%s mid=%.2f spread=%.1f%% "
            "delta=%s OI=%d vol=%d DTE=%d premium=$%.0f contracts=%d score=%.2f",
            ticker, selected.contract_symbol,
            selected.bid, selected.ask, selected.mid,
            selected.spread_pct * 100,
            selected.delta, selected.open_interest,
            selected.volume, selected.dte,
            selected.premium_per_contract,
            selected.affordable_contracts,
            selected.selection_score,
        )
        return selected

    # =========================================================================
    # PRIVATE -- CHAIN FETCH
    # =========================================================================

    def _fetch_chain_with_price(self, ticker: str, direction: str, *, expiration_override: Optional[str] = None) -> tuple[list[dict], Optional[float]]:
        option_type = direction.lower()
        return self._fetch_tradier_chain(ticker, option_type, expiration_override=expiration_override)

    def _fetch_chain(self, ticker: str, direction: str) -> list[dict]:
        chain, _ = self._fetch_chain_with_price(ticker, direction)
        return chain

    def _fetch_tradier_chain(self, ticker: str, option_type: str, *, expiration_override: Optional[str] = None) -> tuple[list[dict], Optional[float]]:
        """Direct Tradier API call for option chain. Returns (chain, underlying_price)."""
        import requests
        # Use broker's pooled session if available (connection reuse, keep-alive).
        # Fall back to bare requests if broker has no session attribute.
        _session = getattr(self.data_broker, "session", None) or requests

        cfg      = getattr(self.data_broker, "cfg", None)
        base_url = (getattr(cfg, "base_url", None)
                    or getattr(self.data_broker, "base_url", "https://sandbox.tradier.com"))
        token = (getattr(cfg, "access_token", None)
                 or getattr(cfg, "token", None)
                 or getattr(self.data_broker, "access_token", None)
                 or getattr(self.data_broker, "token", "")) or ""
        if not token:
            log.error("[%s] No Tradier token found on data_broker -- chain fetch will 401", ticker)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # 1. Fetch underlying quote
        underlying_price = None
        try:
            # P0 (PR #296): throttle before underlying quote GET to prevent
            # market-open stampede from multiple concurrent deferred breaches.
            # No-op when TRADIER_MD_THROTTLE_ENABLED=0 (default).
            try:
                from ap.tradier_market_data_throttle import (
                    before_market_data_call,
                    after_market_data_call,
                )
                before_market_data_call(
                    "/v1/markets/quotes", ticker,
                    context="selector_underlying_quote",
                )
            except Exception:
                pass
            try:
                q_resp = _session.get(
                    f"{base_url}/v1/markets/quotes",
                    params={"symbols": ticker, "greeks": "false"},
                    headers=headers, timeout=8,
                )
                if q_resp.status_code == 200:
                    quotes = q_resp.json().get("quotes", {}).get("quote", {})
                    if isinstance(quotes, dict):
                        underlying_price = float(quotes.get("last") or quotes.get("bid") or 0) or None
            finally:
                try:
                    after_market_data_call()
                except Exception:
                    pass
        except Exception:
            pass

        # 2. Get expirations
        try:
            # P0 (PR #296): throttle before expirations GET.
            try:
                from ap.tradier_market_data_throttle import (
                    before_market_data_call,
                    after_market_data_call,
                )
                before_market_data_call(
                    "/v1/markets/options/expirations", ticker,
                    context="selector_expirations",
                )
            except Exception:
                pass
            try:
                exp_resp = _session.get(
                    f"{base_url}/v1/markets/options/expirations",
                    params={"symbol": ticker, "includeAllRoots": "true"},
                    headers=headers, timeout=10,
                )
            except requests.exceptions.RequestException as _e:
                raise ChainProviderError(f"Expirations fetch network error: {_e}") from _e
            finally:
                try:
                    after_market_data_call()
                except Exception:
                    pass
        except (ChainProviderError, ChainAuthError, ChainEmptyExpirations,
                NoExpirationInDTEWindow):
            raise
        if exp_resp.status_code in (401, 403):
            raise ChainAuthError(
                f"Expirations auth error {exp_resp.status_code} for {ticker}",
                status_code=exp_resp.status_code,
            )
        if exp_resp.status_code != 200:
            raise ChainProviderError(
                f"Expirations fetch failed: HTTP {exp_resp.status_code} for {ticker}",
                status_code=exp_resp.status_code,
            )

        dates = exp_resp.json().get("expirations", {}).get("date", []) or []
        if not dates:
            raise ChainEmptyExpirations(f"[{ticker}] Tradier returned empty expirations list")

        # 3. Pick best expiration (or honor an explicit override from the ladder)
        if expiration_override:
            target_exp = expiration_override if expiration_override in dates else None
            if target_exp is None:
                raise NoExpirationInDTEWindow(
                    f"[{ticker}] expiration_override={expiration_override!r} not found in live expirations"
                )
        else:
            target_exp = self._pick_expiration(dates)
        if not target_exp:
            raise NoExpirationInDTEWindow(
                f"[{ticker}] No valid expiration in DTE window [{self.min_dte},{self.max_dte}] from {len(dates)} dates"
            )

        # 4. Get chain with greeks
        try:
            # P0 (PR #296): throttle before option chain GET.
            try:
                from ap.tradier_market_data_throttle import (
                    before_market_data_call,
                    after_market_data_call,
                )
                before_market_data_call(
                    "/v1/markets/options/chains", ticker,
                    context="selector_chain",
                )
            except Exception:
                pass
            try:
                chain_resp = _session.get(
                    f"{base_url}/v1/markets/options/chains",
                    params={"symbol": ticker, "expiration": target_exp, "greeks": "true"},
                    headers=headers, timeout=10,
                )
            except requests.exceptions.RequestException as _e:
                raise ChainProviderError(f"Chain fetch network error (exp={target_exp}): {_e}") from _e
            finally:
                try:
                    after_market_data_call()
                except Exception:
                    pass
        except (ChainProviderError, ChainAuthError):
            raise
        if chain_resp.status_code in (401, 403):
            raise ChainAuthError(
                f"Chain fetch auth error {chain_resp.status_code} for {ticker}/{target_exp}",
                status_code=chain_resp.status_code,
            )
        if chain_resp.status_code != 200:
            raise ChainProviderError(
                f"Chain fetch failed: HTTP {chain_resp.status_code} for {ticker}/{target_exp}",
                status_code=chain_resp.status_code,
            )

        options = chain_resp.json().get("options", {}).get("option", []) or []
        if not options:
            raise ChainEmptyOptions(
                f"[{ticker}] Tradier returned zero option rows for expiration={target_exp}"
            )

        # 5. Inject ticker into every option dict regardless of quote availability.
        # _quality_filter() uses _ticker for per-ticker premium caps, so this must
        # not depend on underlying_price being present. Inject underlying only when available.
        for o in options:
            o["_ticker"] = ticker
            if underlying_price:
                o["_underlying_price"] = underlying_price

        filtered = [o for o in options if o.get("option_type", "").lower() == option_type]
        return filtered, underlying_price

    # =========================================================================
    # PR1 — DTE-BUCKET LADDER (flag-gated, default off)
    # =========================================================================

    def _is_ladder_eligible(self, plan) -> bool:
        """Whether this plan should use the DTE ladder.

        AMENDMENT (blast-radius fix): the ladder must apply ONLY to deferred
        contract resolution at breach time, never to general selector usage. So
        eligibility requires an EXPLICIT plan-scoped marker set by the deferred
        breach-time path — NOT a broad timeframe heuristic. With this gate, even
        when DEFERRED_DTE_LADDER=1, a normal (non-deferred) select() call is
        byte-for-byte unchanged because no marker is present.

        The marker is read from plan.metadata, accepting either:
            metadata["deferred_breach_selection"] is True   (boolean marker)
            metadata["selection_context"] == "deferred_breach"
        Both object-plans and dict-plans are supported. Never raises.
        """
        try:
            meta = None
            if isinstance(plan, dict):
                meta = plan.get("metadata")
            else:
                meta = getattr(plan, "metadata", None)
            if not isinstance(meta, dict):
                return False
            if meta.get("deferred_breach_selection") is True:
                return True
            if str(meta.get("selection_context") or "") == "deferred_breach":
                return True
        except Exception:
            pass
        return False

    def _preferred_bucket_order(self, plan) -> list[str]:
        """Playbook/timeframe-derived DTE bucket order.

        The playbook CSV carries no explicit DTE column, so the preferred bucket
        is derived from timeframe. Short-dated daily Strat setups prefer the
        nearest liquid expiration first, then adjacent, then 8+ only as last
        resort. This does NOT hard-force 0DTE — it only orders the ladder.
        """
        timeframe = str(_safe_plan_attr(plan, "timeframe", "") or "").lower()
        # 1d / daily / intraday short-dated → near-first ladder.
        # Any longer/unknown timeframe uses the same near-first order by default;
        # the fallback rungs guarantee a quality contract is still found if the
        # near buckets are empty.
        if timeframe in ("1d", "daily", "1day", "d", "60m", "1h", "30m", "15m", "5m", "0dte"):
            return ["A", "B", "C"]
        # Default: still near-first (safe — ladder falls back if empty).
        return ["A", "B", "C"]

    def _fetch_expirations_list(self, ticker: str) -> list[str]:
        """Fetch the raw expirations list for the ticker (live data broker).
        Returns [] on any failure (caller falls back gracefully)."""
        import requests
        _session = getattr(self.data_broker, "session", None) or requests
        cfg      = getattr(self.data_broker, "cfg", None)
        base_url = (getattr(cfg, "base_url", None)
                    or getattr(self.data_broker, "base_url", "https://sandbox.tradier.com"))
        token = (getattr(cfg, "access_token", None)
                 or getattr(cfg, "token", None)
                 or getattr(self.data_broker, "access_token", None)
                 or getattr(self.data_broker, "token", "")) or ""
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        try:
            # P0 (PR #296): throttle before expirations GET in DTE-ladder path.
            try:
                from ap.tradier_market_data_throttle import (
                    before_market_data_call,
                    after_market_data_call,
                )
                before_market_data_call(
                    "/v1/markets/options/expirations", ticker,
                    context="selector_expirations_list",
                )
            except Exception:
                pass
            try:
                resp = _session.get(
                    f"{base_url}/v1/markets/options/expirations",
                    params={"symbol": ticker, "includeAllRoots": "true"},
                    headers=headers, timeout=10,
                )
                if resp.status_code != 200:
                    return []
                return resp.json().get("expirations", {}).get("date", []) or []
            finally:
                try:
                    after_market_data_call()
                except Exception:
                    pass
        except Exception:
            return []

    def _bucket_expirations(self, dates: list[str]) -> dict[str, list[str]]:
        """Group expirations into DTE buckets A (0–a), B (a+1–b), C (b+1+).
        Each bucket's list is sorted nearest-first. Weekends skipped."""
        today = date.today()
        buckets: dict[str, list[tuple[int, str]]] = {"A": [], "B": [], "C": []}
        for d_str in dates:
            try:
                d = date.fromisoformat(d_str)
            except Exception:
                continue
            if d.weekday() >= 5:
                continue
            dte = (d - today).days
            if dte < 0:
                continue
            if dte <= self.dte_bucket_a_max:
                buckets["A"].append((dte, d_str))
            elif dte <= self.dte_bucket_b_max:
                buckets["B"].append((dte, d_str))
            else:
                buckets["C"].append((dte, d_str))
        # nearest-first within each bucket
        return {k: [d for _, d in sorted(v)] for k, v in buckets.items()}

    def _select_with_dte_ladder(self, plan) -> Optional["SelectedContract"]:
        """Evaluate expirations by DTE bucket in playbook-preferred order.

        For each bucket (A→B→C by default), probe its expirations nearest-first
        (up to dte_ladder_probe_per_bucket) and return the FIRST expiration that
        yields a quality survivor. Quality gates are unchanged — this only
        controls which expiration select() evaluates. Records a full audit of
        buckets attempted / survivors per bucket for diagnostics.

        Returns the selected contract, or None (records NO_VALID_PLAYBOOK_DTE_CONTRACT).
        """
        ticker = _safe_plan_attr(plan, "ticker")
        audit: dict = {
            "ladder": True,
            "ticker": ticker,
            "timeframe": str(_safe_plan_attr(plan, "timeframe", "") or ""),
            "bucket_order": [],
            "buckets_attempted": [],
            "selected_bucket": None,
            "selected_dte": None,
            "selected_expiration": None,
        }
        try:
            dates = self._fetch_expirations_list(ticker)
            if not dates:
                # No expirations data — fall back to the legacy single-shot path
                # so we never regress to "no result" purely from a list-fetch miss.
                audit["fallback"] = "no_expirations_list_legacy_single_shot"
                self._last_dte_ladder_audit = audit
                return self.select(plan, expiration_override="")  # "" => legacy pick

            buckets = self._bucket_expirations(dates)
            order = self._preferred_bucket_order(plan)
            audit["bucket_order"] = order
            today = date.today()

            # Terminal non-DTE reason codes: a verdict that cannot improve by
            # trying another expiration. The ladder stops immediately when one
            # is seen because probing further buckets is pointless AND risks
            # masking the true blocker reason.
            #
            # CRITICALLY these must be true non-DTE blockers only — system
            # or account-level verdicts that hold regardless of expiration.
            # Quality codes like OI_TOO_LOW / SPREAD_TOO_WIDE are NOT here
            # because a different expiration might pass those gates; they go
            # into _preserved_quality instead so probing continues.
            _TERMINAL_NON_DTE = {
                "EARNINGS_LOCKOUT", "EARNINGS_GUARD_ERROR",
                "INVALID_PLAN", "UNSUPPORTED_INDEX_MAPPING",
                "CHAIN_AUTH_ERROR",
                "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
                "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                "DELTA_OUT_OF_RANGE",
                "PREMIUM_CAP_EXCEEDED",
            }
            _RETRYABLE_DATA_REASONS = {
                "CHAIN_PROVIDER_ERROR",
                "CHAIN_PROVIDER_EMPTY_OPTIONS",
                "CHAIN_PARSE_EMPTY",
                "NO_EXPIRATION_IN_DTE_WINDOW",
                "NO_CHAIN_DATA",
                "CHAIN_ROW_ZERO_BID_ASK",
                "DIRECT_QUOTE_ZERO_BID_ASK",
                "QUOTE_FETCH_FAILED",
                "CHAIN_EMPTY",
                "DIRECT_QUOTE_UNAVAILABLE",
            }
            _preserved_terminal = None
            _preserved_retryable = None
            _preserved_quality = None

            for bucket_name in order:
                exps = buckets.get(bucket_name, [])[: self.dte_ladder_probe_per_bucket]
                bucket_rec = {"bucket": bucket_name, "expirations_probed": [], "survivor": False}
                for exp in exps:
                    try:
                        _dte = (date.fromisoformat(exp) - today).days
                    except Exception:
                        _dte = None
                    result = self.select(plan, expiration_override=exp)
                    _sub_fail = self._last_failure
                    if result is None and isinstance(_sub_fail, dict):
                        _reason_code = _sub_fail.get("reason_code")
                        if _reason_code in _TERMINAL_NON_DTE:
                            # True non-DTE terminal — stop laddering immediately.
                            _preserved_terminal = dict(_sub_fail)
                            # Record the exp so audit has the failure
                            bucket_rec["expirations_probed"].append({
                                "exp": exp, "dte": _dte, "hit": False,
                                "failure": dict(_sub_fail),
                            })
                            audit["buckets_attempted"].append(bucket_rec)
                            # Short-circuit everything.
                            self._last_failure = _preserved_terminal
                            self._last_dte_ladder_audit = audit
                            log.warning(
                                "[%s] DTE_LADDER_TERMINAL_NON_DTE preserved reason=%s "
                                "— stopping ladder immediately (non-DTE verdict)",
                                ticker, _preserved_terminal.get("reason_code"),
                            )
                            return None
                        elif _reason_code in _RETRYABLE_DATA_REASONS:
                            # Data miss — keep probing, remember this as a candidate
                            # for preservation if no quality reason appears.
                            if _preserved_retryable is None:
                                _preserved_retryable = dict(_sub_fail)
                        elif _reason_code:
                            # Quality reject (OI_TOO_LOW, SPREAD_TOO_WIDE, etc.)
                            # Keep probing — a different expiration might pass.
                            # Remember the most specific quality reason seen.
                            _preserved_quality = dict(_sub_fail)
                    bucket_rec["expirations_probed"].append({
                        "exp": exp, "dte": _dte, "hit": result is not None,
                        "failure": dict(_sub_fail) if result is None and isinstance(_sub_fail, dict) else None,
                    })
                    if result is not None:
                        bucket_rec["survivor"] = True
                        audit["buckets_attempted"].append(bucket_rec)
                        audit["selected_bucket"] = bucket_name
                        audit["selected_dte"] = _dte
                        audit["selected_expiration"] = exp
                        self._last_dte_ladder_audit = audit
                        log.info(
                            "[%s] DTE_LADDER_SELECTED bucket=%s dte=%s exp=%s",
                            ticker, bucket_name, _dte, exp,
                        )
                        return result
                audit["buckets_attempted"].append(bucket_rec)

            # All buckets exhausted. Priority for final reason preservation:
            #   1. Retryable data-miss — upstream retry loop will re-probe after delay.
            #   2. Quality reject — true contract-quality blocker; preserve the most
            #      specific quality reason so dashboards show the real gate, not
            #      the ladder-aggregation NO_VALID_PLAYBOOK_DTE_CONTRACT.
            #   3. NO_VALID_PLAYBOOK_DTE_CONTRACT — fallback when no specific reason
            #      was captured (empty chain, ladder never got any sub-failure).
            #
            # NOTE: _preserved_terminal is handled above via early return; it
            # should be None here.
            self._last_dte_ladder_audit = audit
            if _preserved_retryable is not None:
                self._last_failure = _preserved_retryable
                log.warning(
                    "[%s] DTE_LADDER_RETRYABLE_REASON preserved reason=%s "
                    "— not masking with NO_VALID_PLAYBOOK_DTE_CONTRACT",
                    ticker, _preserved_retryable.get("reason_code"),
                )
                return None
            if _preserved_quality is not None:
                self._last_failure = _preserved_quality
                log.warning(
                    "[%s] DTE_LADDER_QUALITY_REASON preserved reason=%s "
                    "— not masking with NO_VALID_PLAYBOOK_DTE_CONTRACT",
                    ticker, _preserved_quality.get("reason_code"),
                )
                return None
            self._last_failure = {
                "stage": "dte_ladder",
                "reason_code": "NO_VALID_PLAYBOOK_DTE_CONTRACT",
                "explanation": (
                    "No quality survivor in any evaluated DTE bucket "
                    f"(order={order}, buckets={ {k: len(v) for k, v in buckets.items()} })"
                ),
            }
            log.warning(
                "[%s] DTE_LADDER_NO_SURVIVOR order=%s buckets=%s",
                ticker, order, {k: len(v) for k, v in buckets.items()},
            )
            return None
        except Exception as exc:
            # Ladder must never harm the flow — on any unexpected error, fall
            # back to the legacy single-shot selection.
            log.warning("[%s] DTE_LADDER_ERROR falling back to legacy: %s", ticker, exc)
            audit["fallback"] = f"ladder_error_legacy_single_shot:{exc}"
            self._last_dte_ladder_audit = audit
            try:
                return self.select(plan, expiration_override="")
            except Exception:
                return None

    def _pick_expiration(self, dates: list[str]) -> Optional[str]:
        today = date.today()
        valid = []

        for d_str in dates:
            try:
                d = date.fromisoformat(d_str)
                if d.weekday() >= 5:   # skip Saturday (5) and Sunday (6)
                    continue
                dte = (d - today).days
                if self.min_dte <= dte <= self.max_dte:
                    valid.append((dte, d_str))
            except Exception:
                continue

        if not valid:
            for d_str in dates:
                try:
                    d = date.fromisoformat(d_str)
                    if d.weekday() >= 5:   # skip Saturday/Sunday
                        continue
                    dte = (d - today).days
                    if dte >= self.min_dte:
                        valid.append((dte, d_str))
                        break
                except Exception:
                    continue

        if not valid:
            return None

        valid.sort()
        if self.prefer_weekly:
            fridays = [(dte, d) for dte, d in valid if date.fromisoformat(d).weekday() == 4]
            if fridays:
                return fridays[0][1]

        return valid[0][1]

    # =========================================================================
    # PRIVATE -- QUALITY FILTER
    # =========================================================================

    def _synthetic_contract(self, ticker: str, direction: str, reason: str) -> "SelectedContract":
        """Dead code — never called in production. Guard prevents accidental wiring in LIVE.
        If this is ever needed, create a dedicated paper-only code path instead.
        """
        assert self.mode.upper() != "LIVE", (
            "_synthetic_contract must never be called in LIVE mode — "
            f"{ticker}_SIM is not a real option symbol and would be submitted to the broker"
        )
        log.warning("[%s] SYNTHETIC CONTRACT -- %s", ticker, reason)
        return SelectedContract(
            contract_symbol=f"{ticker}_SIM", expiration="SIM", strike=0,
            option_type=direction.lower(), bid=0.50, ask=1.00, mid=0.75,
            spread_pct=0.20, delta=0.50, open_interest=1, volume=1,
            premium_per_share=0.75, premium_per_contract=75.0,
            affordable_contracts=1, selection_reason=reason,
            selection_score=0, dte=0,
        )

    def _quality_filter(
        self,
        opt: dict,
        today: date,
        *,
        max_spread_pct: Optional[float] = None,
        min_oi: Optional[int] = None,
        min_volume: Optional[int] = None,
    ) -> Optional[str]:
        _max_spread = max_spread_pct if max_spread_pct is not None else self.max_spread_pct
        _min_oi     = min_oi         if min_oi         is not None else self.min_oi
        _min_volume = min_volume      if min_volume     is not None else self.min_volume

        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        oi  = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)

        if bid <= 0 or ask <= 0:
            return "zero_bid_or_ask"
        if ask < bid:
            return "ask_below_bid"

        mid = (bid + ask) / 2
        if mid <= 0:
            return "zero_mid"
        spread_pct = (ask - bid) / mid
        if spread_pct > _max_spread:
            return "spread_too_wide_%.1f%%" % (spread_pct * 100)

        if oi < _min_oi:
            return "low_oi_%d" % oi
        if vol < _min_volume:
            return "low_volume_%d" % vol

        premium = mid * 100
        if premium < self.min_premium:
            return "premium_too_low_$%.0f" % premium
        # Per-ticker premium cap (uses _ticker injected in _fetch_tradier_chain)
        _opt_ticker = opt.get("_ticker", "")
        _ticker_max = _get_max_premium(_opt_ticker) if _opt_ticker else self.max_premium
        if premium > _ticker_max:
            return "premium_too_high_$%.0f_max_$%.0f" % (premium, _ticker_max)

        exp_str = opt.get("expiration_date", "")
        if exp_str:
            try:
                exp = date.fromisoformat(exp_str)
                dte = (exp - today).days
                if dte < self.min_dte:
                    return "dte_too_low_%d" % dte
                if dte > self.max_dte:
                    return "dte_too_high_%d" % dte
            except Exception:
                return "invalid_expiration"

        greeks = opt.get("greeks") or {}
        delta, delta_reason = _extract_abs_delta(opt)

        _REQUIRE_REAL_GREEKS = os.getenv("REQUIRE_REAL_GREEKS", "true").lower() != "false"

        if delta is None:
            # Missing/dirty Greeks: use moneyness as fallback if Greeks are not required.
            # When REQUIRE_REAL_GREEKS=true (default), reject the contract outright.
            # This prevents far-OTM $0.11 contracts from passing quality as if delta=target.
            if _REQUIRE_REAL_GREEKS:
                return delta_reason  # e.g. "missing_delta", "dirty_delta_..."
            # Fallback: use moneyness when Greeks are explicitly disabled
            underlying_price = opt.get("_underlying_price")
            strike = float(opt.get("strike") or 0)
            if underlying_price and strike:
                moneyness   = strike / float(underlying_price)
                option_type = opt.get("option_type", "").lower()
                if option_type == "call" and not (0.93 <= moneyness <= 1.12):
                    return "moneyness_out_of_range_%.3f" % moneyness
                if option_type == "put" and not (0.88 <= moneyness <= 1.07):
                    return "moneyness_out_of_range_%.3f" % moneyness
            else:
                return delta_reason
        else:
            min_d = max(0.05, self.target_delta - self.delta_band)
            max_d = min(0.95, self.target_delta + self.delta_band)
            if delta < min_d or delta > max_d:
                return "delta_out_of_band_%.2f" % delta

        return None

    # =========================================================================
    # PRIVATE -- RANKING
    # =========================================================================

    def _rank_score(self, opt: dict, budget: float,
                    expected_move_pct: float = 0.0,
                    underlying_price: float = 0.0,
                    tier: str = "B") -> float:
        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        oi  = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)
        mid = (bid + ask) / 2
        if mid <= 0:
            return -9999.0

        spread_pct = (ask - bid) / mid
        # Ranking still scores value/liquidity from midpoint, but affordability
        # must use the same capital basis as _build_selected(): ask in LIVE, mid
        # in paper/research. This prevents a contract from ranking highly only
        # because it fits at mid while failing live affordability at ask.
        is_live, _pricing_basis = _pricing_basis_for_mode(getattr(self, "mode", "paper"))
        execution_price = ask if is_live else mid
        execution_premium = execution_price * 100

        greeks = opt.get("greeks") or {}
        delta, delta_reason = _extract_abs_delta(opt)
        if delta is None:
            # Missing Greeks: assign worst possible delta + heavy penalty.
            # NEVER use target_delta as fallback — that silently makes
            # a $0.11 far-OTM contract score like it has perfect delta.
            delta = 0.01
            missing_greeks_penalty = -300.0
        else:
            missing_greeks_penalty = 0.0

        # Premium quality penalties — punish both extremes.
        # MIN_IDEAL: $75/contract — below this fills are fragile (1 penny = -9% loss).
        # MAX_IDEAL: $350/contract — above this uses too much capital.
        MIN_IDEAL_PREMIUM = float(os.getenv("MIN_IDEAL_PREMIUM_PER_CONTRACT", "75"))  / 100.0
        MAX_IDEAL_PREMIUM = float(os.getenv("MAX_IDEAL_PREMIUM_PER_CONTRACT", "350")) / 100.0
        too_cheap_penalty     = max(0.0, MIN_IDEAL_PREMIUM - execution_price) * 150.0
        too_expensive_penalty = max(0.0, execution_price - MAX_IDEAL_PREMIUM) * 25.0
        premium_penalty       = too_cheap_penalty + too_expensive_penalty

        effective_budget, _, _ = _effective_budget(budget)
        affordable = int(effective_budget / execution_premium) if execution_premium > 0 else 0
        affordability_ratio = min(1.0, effective_budget / execution_premium) if execution_premium > 0 else 0.0
        unaffordable_penalty = -500.0 if affordable < 1 else 0.0

        delta_distance = abs(delta - self.target_delta)

        otm_bias = 0.0
        if underlying_price > 0:
            strike = float(opt.get("strike") or 0)
            if strike <= underlying_price:
                otm_bias = 0.2
            elif expected_move_pct >= 1.5:
                otm_dist = (strike - underlying_price) / underlying_price * 100
                if otm_dist <= 0.5:
                    otm_bias = 0.1

        if tier in ("A+", "A"):
            delta_weight   = -140
            spread_weight  =  -90
            premium_weight =  -25
        elif tier == "B":
            delta_weight   = -130
            spread_weight  =  -80
            premium_weight =  -35
        else:
            delta_weight   = -120
            spread_weight  =  -70
            premium_weight =  -50

        return (
            (delta_distance  * delta_weight)   +
            (spread_pct      * spread_weight)  +
            (math.log(oi+1)  * 12)             +
            (math.log(vol+1) *  6)             +
            - premium_penalty                  +
            (affordability_ratio * 10)         +
            unaffordable_penalty               +
            missing_greeks_penalty             +
            (otm_bias        *  6)
        )

    # =========================================================================
    # PRIVATE -- BUILD SelectedContract
    # =========================================================================

    def _build_selected(self, opt: dict, score: float, budget: float, today: date) -> Optional[SelectedContract]:
        try:
            bid = float(opt.get("bid") or 0)
            ask = float(opt.get("ask") or 0)
            mid = (bid + ask) / 2
            spread_pct = (ask - bid) / mid if mid > 0 else 0

            greeks = opt.get("greeks") or {}
            try:
                delta = abs(float(greeks.get("delta") or 0)) or None
            except Exception:
                delta = None

            oi  = int(opt.get("open_interest") or 0)
            vol = int(opt.get("volume") or 0)

            exp_str = opt.get("expiration_date", "")
            try:
                dte = (date.fromisoformat(exp_str) - today).days
            except Exception:
                dte = 0

            # ── P1 ENTRY PRICING (2026-05-21) ─────────────────────────────
            # Observed funnel: 348 canceled / 44 expired / only 2 filled.
            # Old spread-adaptive blend (mid / mid+40% / ask) sat unfilled
            # 150s+ in production. Fix: LIVE Attempt-0 starts at ASK; the
            # repeg ladder handles ask+0.01, ask+0.02 if not filled.
            #
            # Scoring/ranking still uses mid (consistent across names).
            # Paper mode keeps mid simulation so backtests are comparable.
            #
            # ENTRY_ATTEMPT0_PRICING env var ('ASK' default | 'BLEND' legacy)
            # lets ops fall back to the old blend if needed without a redeploy.
            # ─────────────────────────────────────────────────────────────
            is_live, pricing_basis = _pricing_basis_for_mode(getattr(self, "mode", "paper"))
            scoring_price_per_share = mid   # ranking always on mid (unchanged)

            _attempt0_mode = os.getenv("ENTRY_ATTEMPT0_PRICING", "ASK").strip().upper()
            if is_live and _attempt0_mode == "ASK" and ask > 0:
                # P1 fix: live Attempt-0 starts at ask. Repeg ladder handles
                # any extra slippage if the ask itself walks up.
                execution_price_per_share = ask
                pricing_basis = "ASK_EXECUTION"
            elif ask <= 0:
                execution_price_per_share = mid or bid or 0.01
            elif spread_pct < 0.08:
                # Legacy / paper / opted-out: tight spread → mid
                execution_price_per_share = mid
            elif spread_pct < 0.20:
                # Legacy / paper / opted-out: normal spread → mid + 40%
                execution_price_per_share = round(mid + (ask - mid) * 0.40, 2)
            else:
                execution_price_per_share = ask

            # Floor: never price below bid (would be absurd) or above ask
            execution_price_per_share = max(
                min(execution_price_per_share, ask),
                bid if bid > 0 else execution_price_per_share
            )
            execution_price_per_share = round(execution_price_per_share, 2)
            premium_per_share         = execution_price_per_share
            premium_per_contract      = execution_price_per_share * 100
            effective_budget, MAX_TRADE_USD, budget_clipped = _effective_budget(budget)
            _raw_affordable           = int(effective_budget / premium_per_contract) if premium_per_contract > 0 else 0
            # Operational hard cap: never exceed MAX_CONTRACTS per position.
            # Must agree with ap.execution.MAX_CONTRACTS (same env var)
            # or the selector silently caps below the sizer's cap, hiding
            # the true exposure ceiling.
            # PR #30 (2026-05-23): default raised 6 → 15 for proof week.
            _MAX_CONTRACTS_HARD_CAP   = int(os.getenv("MAX_CONTRACTS", "15"))
            affordable                = min(_raw_affordable, _MAX_CONTRACTS_HARD_CAP)

            return SelectedContract(
                contract_symbol      = opt.get("symbol", ""),
                expiration           = exp_str,
                strike               = float(opt.get("strike") or 0),
                option_type          = opt.get("option_type", "").lower(),
                bid                  = bid,
                ask                  = ask,
                mid                  = mid,
                spread_pct           = spread_pct,
                delta                = delta,
                open_interest        = oi,
                volume               = vol,
                premium_per_share    = premium_per_share,
                premium_per_contract = premium_per_contract,
                affordable_contracts = affordable,
                selection_reason     = (
                    "delta=%.2f spread=%.1f%% OI=%d vol=%d DTE=%d premium=$%.0f" % (
                        delta, spread_pct*100, oi, vol, dte, premium_per_contract
                    ) if delta else
                    "spread=%.1f%% OI=%d vol=%d DTE=%d premium=$%.0f" % (
                        spread_pct*100, oi, vol, dte, premium_per_contract
                    )
                ),
                selection_score      = score,
                dte                  = dte,
                scoring_price_per_share   = scoring_price_per_share,
                execution_price_per_share = execution_price_per_share,
                effective_budget          = effective_budget,
                budget_clipped            = budget_clipped,
                pricing_basis             = pricing_basis,
            )
        except Exception as e:
            log.error("_build_selected failed: %s", e)
            return None
