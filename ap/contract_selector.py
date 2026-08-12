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
import contextvars
from ap.trace import trace_gate
import os
import time
from dataclasses import dataclass, field, replace as _dc_replace
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo
from ap.contract_playbook import (
    ContractPlaybookSpec,
    STRIKE_POLICY_LABEL_ATM,
    STRIKE_POLICY_LABEL_NONMATCH,
    STRIKE_POLICY_LABEL_ONE_STEP_OTM,
    STRIKE_POLICY_TIER_ATM,
    STRIKE_POLICY_TIER_NONMATCH,
    STRIKE_POLICY_TIER_ONE_STEP_OTM,
    TRIGGER_ANCHOR_SOURCE_NONE,
    TRIGGER_ANCHOR_SOURCE_TRIGGER,
    TRIGGER_ANCHOR_SOURCE_UNDERLYING_FALLBACK,
    build_playbook_candidate_context,
    playbook_contract_selection_enabled,
    resolve_contract_playbook,
    resolve_playbook_expiration_order,
    resolve_trigger_anchored_preferred_strikes,
)

# P0A: direct option quote revalidation (do not remove — restores flow for
# liquid tickers with stale/zero chain data without weakening safety rules).
from ap.contract_quote_revalidator import (
    revalidate_with_direct_quote  as _revalidate_direct,
    should_revalidate             as _should_revalidate,
    correct_recovered_cursor_disposition as _correct_recovered_cursor_disposition,
    _ctx_persist_structural_skip,
    _ctx_persist_attempt,
    DEFAULT_REVALIDATE_TOP_N,
)

# ── P0: exact chain-fetch taxonomy exceptions ────────────────────────────────
class ChainProviderError(Exception):
    """HTTP or network error from the options chain provider."""
    def __init__(
        self,
        msg: str,
        *,
        status_code: int | None = None,
        provider_latency_ms: float | None = None,
        retry_after_ms: int | None = None,
        attempts: int = 1,
    ):
        super().__init__(msg)
        self.status_code = status_code
        self.provider_latency_ms = provider_latency_ms
        self.retry_after_ms = retry_after_ms
        self.attempts = attempts


class ChainAuthError(ChainProviderError):
    """401/403 from the options chain provider — credential/token failure."""
    pass


class ChainEmptyExpirations(Exception):
    """Expirations endpoint returned an empty list for this ticker."""
    def __init__(
        self,
        msg: str,
        *,
        status_code: int | None = 200,
        provider_latency_ms: float | None = None,
        retry_after_ms: int | None = None,
        attempts: int = 1,
    ):
        super().__init__(msg)
        self.status_code = status_code
        self.provider_latency_ms = provider_latency_ms
        self.retry_after_ms = retry_after_ms
        self.attempts = attempts


class NoExpirationInDTEWindow(Exception):
    """Expirations exist but none fall within the configured DTE window."""
    pass


class ChainEmptyOptions(Exception):
    """Options chain endpoint returned zero rows for this expiration."""
    pass


class ChainParseEmpty(Exception):
    """Options chain response parsed to zero rows after direction filter."""
    pass


class SelectorRequestBudgetExhausted(Exception):
    """A hard per-request selector provider-call or elapsed limit was reached."""
    reason_code = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

    def __init__(self, stage: str, detail: str):
        super().__init__(f"{stage}: {detail}")
        self.stage = stage
        self.detail = detail


@dataclass(frozen=True)
class DirectQuoteBudgetConfig:
    canonical_raw: str | None
    direct_recovery_raw: str | None
    contract_revalidate_raw: str | None
    effective_limit: int
    source: str
    conflict: bool
    conflict_detail: str | None


@dataclass
class SelectorRequestContext:
    ticker: str
    underlying_price: float | None = None
    expirations: list[str] | None = None
    direct_quote_attempts_remaining: int = 0
    provider_call_counts: dict[str, int] = field(default_factory=dict)
    elapsed_ms_by_stage: dict[str, float] = field(default_factory=dict)
    revalidated_contracts: set[str] = field(default_factory=set)
    throttle_wait_ms: float = 0.0
    throttle_diagnostics: list[dict] = field(default_factory=list)
    expirations_probed: list[str] = field(default_factory=list)
    started_at_monotonic: float = 0.0
    legacy_fallback_used: bool = False
    execution_mode: str = "unknown"
    selector_request_kind: str = "ORDINARY"
    max_expiration_calls: int = 2
    max_chain_calls: int = 6
    # Generic dataclass defaults describe an ORDINARY request with no canonical
    # capacity env present — the pre-PR #401 default of five direct quotes.
    # Deferred-recovery contexts receive their explicit computed value in
    # _new_selector_request_context() and never depend on these defaults.
    max_direct_quote_calls: int = 5
    effective_direct_quote_limit: int = 5
    direct_quote_budget_source: str = "default"
    direct_quote_budget_conflict: bool = False
    direct_quote_budget_conflict_detail: str | None = None
    configured_selector_max_direct_quote_calls: int | None = None
    configured_direct_quote_recovery_top_n: int | None = None
    configured_contract_revalidate_top_n: int | None = None
    # Rows that are at least structurally direct-quotable (valid OCC, valid
    # expiration, directional fit). Set at chain-ordering time. Duplicate raw
    # rows remain visible here for quality diagnostics; actual provider calls
    # are separately fenced by normalized OCC identity.
    direct_quote_structural_candidates: int = 0
    # Rows whose chain-reject reason was one _should_revalidate() accepted —
    # i.e., the ones that actually reached the direct-quote branch. Set in
    # the quality-filter loop, once per unique OCC symbol.
    direct_quote_eligible_candidates: int = 0
    direct_quote_eligible_symbols: set[str] = field(default_factory=set)
    direct_quote_attempted_symbols: list[str] = field(default_factory=list)
    direct_quote_unattempted_symbols: list[str] = field(default_factory=list)
    direct_quote_unattempted_set: set[str] = field(default_factory=set)
    direct_quote_unattempted_count: int = 0
    direct_quote_candidate_ranking: list[dict] = field(default_factory=list)
    direct_quote_duplicate_symbols: list[str] = field(default_factory=list)
    # A normalized OCC can appear more than once with independently valid but
    # decision-relevant price or liquidity values. Those rows are not
    # financially authoritative; select() must obtain one normalized direct
    # quote and reuse it for every representation before quality/affordability.
    duplicate_quote_conflicts: list[dict] = field(default_factory=list)
    duplicate_quote_conflict_symbols: set[str] = field(default_factory=set)
    duplicate_quote_conflict_dimensions: dict[str, tuple[str, ...]] = field(
        default_factory=dict
    )
    duplicate_quote_authority: dict[str, dict] = field(default_factory=dict)
    duplicate_quote_authority_attempted: set[str] = field(default_factory=set)
    duplicate_quote_authority_failures: dict[str, str] = field(default_factory=dict)
    max_total_elapsed_ms: int = 15000
    budget_exhausted_stage: str | None = None
    budget_exhausted_detail: str | None = None
    expiration_http_status: int | None = None
    expiration_provider_latency_ms: float | None = None
    retry_after_ms: int | None = None
    diagnostics_sink: dict | None = None
    playbook_spec: ContractPlaybookSpec | None = None
    playbook_ordered_expirations: list[str] = field(default_factory=list)
    playbook_candidate_context: dict | None = None
    playbook_audit: dict | None = None
    playbook_enabled: bool | None = None
    playbook_now_et: datetime | None = None
    playbook_today_et: date | None = None
    recovery_attempt_number: int = 1
    recovery_cursor: dict | None = None
    recovery_cursor_persist: object | None = None
    structural_skips: list[dict] = field(default_factory=list)
    selector_candidate_universe_count: int = 0
    selector_candidate_accounted_count: int = 0
    selector_candidate_accounting_complete: bool = False
    selector_candidate_outcomes: dict[str, str] = field(default_factory=dict)
    affordability_headroom_pct: float = 0.10
    symbol_refresh_seconds: int = 20


SELECTOR_REQUEST_KIND_ORDINARY = "ORDINARY"
SELECTOR_REQUEST_KIND_DEFERRED_BREACH = "DEFERRED_BREACH_MATERIALIZATION"

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
    "moneyness_out_of_range": "MONEYNESS_OUT_OF_RANGE",  # own code: delta may be valid, only strike distance failed
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
    "CHAIN_AUTH_ERROR":               "NO_CHAIN_DATA",
    "CHAIN_PROVIDER_ERROR":           "NO_CHAIN_DATA",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS": "NO_CHAIN_DATA",
    "DTE_LADDER_ERROR":               "NO_CHAIN_DATA",
    "SELECTOR_REQUEST_BUDGET_EXHAUSTED": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
    "MARKET_DATA_THROTTLE_UNAVAILABLE": "MARKET_DATA_THROTTLE_UNAVAILABLE",
    "QUOTE_FETCH_FAILED":             "QUOTE_FETCH_FAILED",
    # Zero-quote rows — data was returned but bid/ask is unusable
    "CHAIN_ROW_ZERO_BID_ASK":         "QUOTE_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK":      "QUOTE_ZERO_BID_ASK",
    "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
    # 1:1 pass-through — reason is already dashboard-safe and actionable
    "BID_BELOW_MIN":                  "BID_BELOW_MIN",
    "SPREAD_TOO_WIDE":                "SPREAD_TOO_WIDE",
    "OI_TOO_LOW":                     "OI_TOO_LOW",
    "VOLUME_TOO_LOW":                 "VOLUME_TOO_LOW",
    "DELTA_OUT_OF_RANGE":             "DELTA_OUT_OF_RANGE",
    "MONEYNESS_OUT_OF_RANGE":         "MONEYNESS_OUT_OF_RANGE",
    "DTE_OUT_OF_RANGE":               "DTE_OUT_OF_RANGE",
    "CHEAP_CONTRACT_NO_UPGRADE":      "NO_AFFORDABLE_CONTRACT",
    "CHEAP_CONTRACT_ONLY_CHOICE":     "CHEAP_CONTRACT_ONLY_CHOICE",
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


# One shared, explicit precedence list keeps final-gate and DTE-ladder
# reductions from depending on dictionary insertion order or whichever
# expiration happened to be evaluated last.
_SELECTOR_FINAL_REASON_PRECEDENCE: tuple[str, ...] = (
    "CAPITAL_NO_REMAINING",
    "INVALID_POSITION_BUDGET",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "DELTA_OUT_OF_RANGE",
    "MONEYNESS_OUT_OF_RANGE",
    "CHEAP_CONTRACT_NO_UPGRADE",
    "PREMIUM_CAP_EXCEEDED",
    "PAPER_PREMIUM_CAP",
    "CONTRACT_BUILD_FAILED",
    "NO_AFFORDABLE_CONTRACT",
)

_DTE_QUALITY_REASON_PRECEDENCE: tuple[str, ...] = (
    *_SELECTOR_FINAL_REASON_PRECEDENCE,
    "DTE_OUT_OF_RANGE",
    "BID_BELOW_MIN",
    "SPREAD_TOO_WIDE",
    "OI_TOO_LOW",
    "VOLUME_TOO_LOW",
    "NO_CONTRACT_AFTER_FILTERS",
)


def _dte_failure_reason(failure: dict | None) -> str:
    if not isinstance(failure, dict):
        return "UNKNOWN_REJECTION"
    return str(
        failure.get("canonical_selector_reason")
        or failure.get("reason_code")
        or "UNKNOWN_REJECTION"
    ).strip() or "UNKNOWN_REJECTION"


def _choose_stronger_dte_quality_failure(
    current: dict | None,
    candidate: dict | None,
) -> dict | None:
    """Reduce DTE quality failures deterministically.

    The first failure wins only when both reasons have the same explicit
    precedence. Different reasons use the fixed policy order above, so a
    later expiration cannot replace an economically stronger earlier truth
    merely because it was observed later.
    """
    if not isinstance(current, dict):
        return dict(candidate) if isinstance(candidate, dict) else None
    if not isinstance(candidate, dict):
        return dict(current)
    _rank = {
        reason: index for index, reason in enumerate(_DTE_QUALITY_REASON_PRECEDENCE)
    }
    _current_reason = _dte_failure_reason(current)
    _candidate_reason = _dte_failure_reason(candidate)
    _current_key = (
        _rank.get(_current_reason, len(_rank)),
        _current_reason,
    )
    _candidate_key = (
        _rank.get(_candidate_reason, len(_rank)),
        _candidate_reason,
    )
    return dict(current if _current_key <= _candidate_key else candidate)


def _merge_dte_operational_failure(
    canonical_failure: dict,
    operational_failure: dict,
) -> dict:
    """Keep a prior selector verdict while recording the later stop reason."""
    _canonical = dict(canonical_failure or {})
    _operational = dict(operational_failure or {})
    _canonical_reason = _dte_failure_reason(_canonical)
    _operational_reason = _dte_failure_reason(_operational)
    _canonical["reason_code"] = _canonical_reason
    _canonical["canonical_selector_reason"] = _canonical_reason
    _canonical["selector_terminal_reason"] = _canonical_reason
    # This is intentionally the latest observed reason, not the canonical
    # decision. Consumers can see both dimensions without inference.
    _canonical["last_observed_selector_reason"] = _operational_reason
    _canonical["operational_reason"] = _operational_reason
    _canonical["operational_selector_reason"] = _operational_reason
    _canonical["operational_queue_reason_code"] = _to_queue_reason(_operational_reason)
    _canonical["queue_reason_code"] = _to_queue_reason(_canonical_reason)
    _canonical["operational_failure"] = _operational
    _base_explanation = str(_canonical.get("explanation") or "").strip()
    _stop_explanation = str(_operational.get("explanation") or "").strip()
    if _stop_explanation:
        _canonical["explanation"] = (
            f"{_base_explanation} | ladder_stop={_operational_reason}: {_stop_explanation}"
            if _base_explanation
            else f"ladder_stop={_operational_reason}: {_stop_explanation}"
        )

    _base_diag = _canonical.get("selection_diagnostics")
    _operational_diag = _operational.get("selection_diagnostics")
    if isinstance(_base_diag, dict) or isinstance(_operational_diag, dict):
        _merged_diag = dict(_base_diag or {})
        _merged_diag.update(dict(_operational_diag or {}))
        _canonical["selection_diagnostics"] = _merged_diag
    return _canonical


def _attach_selector_failure(
    plan,
    *,
    reason_code: str,
    canonical_selector_reason: str | None = None,
    last_observed_selector_reason: str | None = None,
    operational_reason: str | None = None,
    explanation: str,
    chain_rows: int = 0,
    survivor_count: int = 0,
    reject_buckets: "dict | None" = None,
    base_url: str = "",
    execution_mode: str = "unknown",
    best_rejected_candidate: "dict | None" = None,
    chain_quote_validity: "dict | None" = None,           # P0 PR #302 Fix 4
    direct_quote_recovery_audit: "dict | None" = None,   # P0 PR #302 Fix 2
    selection_diagnostics: "dict | None" = None,
) -> None:
    """Attach plan.metadata["selector_failure"] when select() returns None.

    PR P1 — selector reason honesty. Provides a structured, queryable failure
    dict so callers (e.g. PR #183 write-back in ap_execution_core) can surface
    truthful reason codes to trade_queue.last_error and the operator dashboard.

    Fields:
        reason_code        — canonical internal code (e.g. CHAIN_ROW_ZERO_BID_ASK)
        canonical_selector_reason — selector-owned reduced reason consumed by retry owners
        last_observed_selector_reason — most recent raw stage/candidate observation
        operational_reason — separate request-budget/throttle reason, when present
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
        _canonical = str(canonical_selector_reason or _rc).strip() or _rc
        _last_observed = (
            str(last_observed_selector_reason or _rc).strip() or _canonical
        )
        _operational = str(operational_reason or "").strip() or None
        _qrc = _to_queue_reason(_canonical)
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
            "reason_code":        _canonical,
            "canonical_selector_reason": _canonical,
            "last_observed_selector_reason": _last_observed,
            "operational_reason": _operational,
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
                _canonical,
                chain_quote_validity=chain_quote_validity,
                sandbox_mode="sandbox" in _base.lower(),
                execution_mode=str(execution_mode or "unknown"),
            )
            failure["selector_failure_class"]   = _fclass
            failure["selector_reason_detail"]   = str(explanation or "")
            failure["selector_terminal_reason"] = _canonical
            failure["data_failure"]             = _dfail
            failure["quality_failure"]          = _qfail
        except Exception:
            pass

        # P0 PR #302 Fix 4: chain quote validity summary
        if chain_quote_validity:
            failure["selector_chain_quote_validity"] = chain_quote_validity

        # P0 PR #302 Fix 2: direct quote recovery audit — pass through all fields
        # including quality_recheck_failed so operators can distinguish
        # "zero bid/ask" from "real prices but spread/OI gate re-fired".
        if direct_quote_recovery_audit:
            _dqra = direct_quote_recovery_audit
            failure.update({
                "direct_quote_recovery_attempted":  bool(_dqra.get("attempted", False)),
                "direct_quote_recovery_selected":   bool(_dqra.get("selected", False)),
                "direct_quote_recovery_contract":   _dqra.get("contract"),
                "direct_quote_recovery_bid":        _dqra.get("bid"),
                "direct_quote_recovery_ask":        _dqra.get("ask"),
                "direct_quote_recovery_mid":        _dqra.get("mid"),
                "direct_quote_recovery_failure":    _dqra.get("failure"),
                # Bug 2 fix: quality re-failure fields — set only when direct quote
                # returned real prices but the re-run quality filter still rejected.
                "quality_recheck_failed":           bool(_dqra.get("quality_recheck_failed", False)),
                "direct_bid_at_recheck":            _dqra.get("direct_bid_at_recheck"),
                "direct_ask_at_recheck":            _dqra.get("direct_ask_at_recheck"),
            })

        if selection_diagnostics:
            failure["selection_diagnostics"] = selection_diagnostics

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


def _clear_selector_failure(plan) -> None:
    """Remove selector-owned failure metadata from a plan, if present."""
    try:
        if isinstance(plan, dict):
            meta = plan.get("metadata")
            if isinstance(meta, dict):
                meta.pop("selector_failure", None)
            return
        meta = getattr(plan, "metadata", None)
        if isinstance(meta, dict):
            meta.pop("selector_failure", None)
    except Exception:
        pass


def _get_selector_failure(plan) -> dict | None:
    """Return a defensive copy of plan.metadata['selector_failure'], if present."""
    try:
        if isinstance(plan, dict):
            meta = plan.get("metadata")
        else:
            meta = getattr(plan, "metadata", None)
        if isinstance(meta, dict):
            failure = meta.get("selector_failure")
            if isinstance(failure, dict):
                return dict(failure)
    except Exception:
        pass
    return None


def _restore_selector_failure(plan, failure: dict | None) -> None:
    """Restore a previously captured selector_failure snapshot onto the plan."""
    try:
        if not isinstance(failure, dict):
            _clear_selector_failure(plan)
            return
        if isinstance(plan, dict):
            plan.setdefault("metadata", {})["selector_failure"] = dict(failure)
            return
        meta = getattr(plan, "metadata", None)
        if not isinstance(meta, dict):
            meta = {}
            try:
                setattr(plan, "metadata", meta)
            except Exception:
                return
        meta["selector_failure"] = dict(failure)
    except Exception:
        pass


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


def _positive_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(str(os.getenv(name, str(default))).strip()))
    except (TypeError, ValueError):
        return default


def _bounded_float_env(name: str, default: float, low: float, high: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        log.warning(
            "SELECTOR_ENV_PARSE_ERROR key=%s value=%r expected_type=float default=%s",
            name,
            raw,
            default,
        )
        return float(default)
    if not math.isfinite(value) or value < low or value > high:
        log.warning(
            "SELECTOR_ENV_PARSE_ERROR key=%s value=%r expected_range=[%s,%s] default=%s",
            name,
            raw,
            low,
            high,
            default,
        )
        return float(default)
    return value


_DIRECT_QUOTE_BUDGET_CONFLICTS_LOGGED: set[str] = set()


def _parse_positive_int_config(env, key: str) -> tuple[int | None, str | None]:
    raw = env.get(key)
    if raw is None or str(raw).strip() == "":
        return None, None
    raw_s = str(raw).strip()
    try:
        parsed = int(raw_s)
    except (TypeError, ValueError):
        log.warning(
            "SELECTOR_ENV_PARSE_ERROR key=%s value=%r expected_type=positive_int",
            key,
            raw_s,
        )
        return None, raw_s
    if parsed <= 0:
        log.warning(
            "SELECTOR_ENV_PARSE_ERROR key=%s value=%r expected_type=positive_int",
            key,
            raw_s,
        )
        return None, raw_s
    return parsed, raw_s


def _resolve_direct_quote_budget_config(env=None) -> DirectQuoteBudgetConfig:
    env = os.environ if env is None else env
    canonical, canonical_raw = _parse_positive_int_config(env, "SELECTOR_MAX_DIRECT_QUOTE_CALLS")
    direct_recovery, direct_recovery_raw = _parse_positive_int_config(env, "DIRECT_QUOTE_RECOVERY_TOP_N")
    contract_revalidate, contract_revalidate_raw = _parse_positive_int_config(env, "CONTRACT_REVALIDATE_TOP_N")

    conflict = False
    conflict_detail = None
    if canonical is not None:
        effective = canonical
        source = "SELECTOR_MAX_DIRECT_QUOTE_CALLS"
        conflicting = []
        if direct_recovery is not None and direct_recovery != canonical:
            conflicting.append(f"direct_recovery={direct_recovery}")
        if contract_revalidate is not None and contract_revalidate != canonical:
            conflicting.append(f"contract_revalidate={contract_revalidate}")
        conflict = bool(conflicting)
        if conflict:
            conflict_detail = " ".join(conflicting)
            log_key = (
                f"canonical={canonical} direct_recovery={direct_recovery} "
                f"contract_revalidate={contract_revalidate}"
            )
            if log_key not in _DIRECT_QUOTE_BUDGET_CONFLICTS_LOGGED:
                _DIRECT_QUOTE_BUDGET_CONFLICTS_LOGGED.add(log_key)
                log.critical(
                    "SELECTOR_DIRECT_QUOTE_BUDGET_CONFLICT canonical=%s direct_recovery=%s "
                    "contract_revalidate=%s effective=%s source=%s",
                    canonical,
                    direct_recovery,
                    contract_revalidate,
                    effective,
                    source,
                )
    else:
        # The legacy aliases remain diagnostic-only.  They must never become a
        # second behavioral budget authority when the canonical value is
        # absent or malformed.
        effective = 40
        source = "default"

    return DirectQuoteBudgetConfig(
        canonical_raw=canonical_raw,
        direct_recovery_raw=direct_recovery_raw,
        contract_revalidate_raw=contract_revalidate_raw,
        effective_limit=int(effective),
        source=source,
        conflict=conflict,
        conflict_detail=conflict_detail,
    )


def _new_selector_request_context(
    ticker: str,
    execution_mode: str = "unknown",
    *,
    selector_request_kind: str = SELECTOR_REQUEST_KIND_ORDINARY,
    recovery_attempt_number: int = 1,
    recovery_cursor: dict | None = None,
    recovery_cursor_persist=None,
) -> SelectorRequestContext:
    request_kind = str(
        selector_request_kind or SELECTOR_REQUEST_KIND_ORDINARY
    ).strip().upper()
    deferred_recovery = request_kind == SELECTOR_REQUEST_KIND_DEFERRED_BREACH
    budget_cfg = _resolve_direct_quote_budget_config()
    # PR #401 recovery capacity is explicit deferred-breach behavior.  Ordinary
    # selection retains the pre-PR production default of FIVE direct quotes when
    # the canonical env is absent — never the deferred-recovery capacity.
    #
    # SELECTOR_MAX_DIRECT_QUOTE_CALLS is the canonical behavioral quote-call
    # authority (Amendment 5).  For ordinary requests it operates as a CEILING:
    # the env can raise the ordinary cap up to 20 or lower it (e.g. test probes,
    # budget-exhaustion tests) but must never raise it above 20.  For
    # deferred-breach requests the full env value is the budget (Amendment 4:
    # default 40).
    #
    # Examples:
    #   deferred, no env            → 40
    #   deferred, canonical env=20  → 20
    #   deferred, canonical env=40  → 40
    #   ordinary, no env            → 5   (pre-PR #401 default restored)
    #   ordinary, canonical env=1   → min(20,  1) =  1
    #   ordinary, canonical env=20  → min(20, 20) = 20
    #   ordinary, canonical env=40  → min(20, 40) = 20
    if deferred_recovery:
        effective_direct_quote_limit = budget_cfg.effective_limit
    elif budget_cfg.source == "SELECTOR_MAX_DIRECT_QUOTE_CALLS":
        # Canonical env explicitly set: apply as a ceiling for ordinary.
        effective_direct_quote_limit = min(20, budget_cfg.effective_limit)
    else:
        # Ordinary default with no canonical env: pre-PR #401 default of five.
        effective_direct_quote_limit = 5
    context = SelectorRequestContext(
        ticker=str(ticker or ""),
        direct_quote_attempts_remaining=effective_direct_quote_limit,
        started_at_monotonic=time.monotonic(),
        execution_mode=str(execution_mode or "unknown").lower(),
        selector_request_kind=request_kind,
        # Ordinary defaults stay pre-PR #401 (expiration 2, chain 6, elapsed
        # 15000ms), but the explicit environment overrides remain authoritative
        # for ordinary requests too — restoring the base-SHA behavior. Deferred
        # recovery keeps its higher defaults (3 / 8 / 25000ms).
        max_expiration_calls=(
            _positive_int_env("SELECTOR_MAX_EXPIRATION_CALLS", 3)
            if deferred_recovery
            else _positive_int_env("SELECTOR_MAX_EXPIRATION_CALLS", 2)
        ),
        max_chain_calls=(
            _positive_int_env("SELECTOR_MAX_CHAIN_CALLS", 8)
            if deferred_recovery
            else _positive_int_env("SELECTOR_MAX_CHAIN_CALLS", 6)
        ),
        max_direct_quote_calls=effective_direct_quote_limit,
        effective_direct_quote_limit=effective_direct_quote_limit,
        direct_quote_budget_source=(
            budget_cfg.source if deferred_recovery else "ordinary_pre_pr_envelope"
        ),
        direct_quote_budget_conflict=budget_cfg.conflict,
        direct_quote_budget_conflict_detail=budget_cfg.conflict_detail,
        configured_selector_max_direct_quote_calls=(
            int(budget_cfg.canonical_raw) if budget_cfg.canonical_raw and budget_cfg.canonical_raw.isdigit() else None
        ),
        configured_direct_quote_recovery_top_n=(
            int(budget_cfg.direct_recovery_raw) if budget_cfg.direct_recovery_raw and budget_cfg.direct_recovery_raw.isdigit() else None
        ),
        configured_contract_revalidate_top_n=(
            int(budget_cfg.contract_revalidate_raw) if budget_cfg.contract_revalidate_raw and budget_cfg.contract_revalidate_raw.isdigit() else None
        ),
        max_total_elapsed_ms=(
            _positive_int_env("SELECTOR_MAX_TOTAL_ELAPSED_MS", 25000)
            if deferred_recovery
            else _positive_int_env("SELECTOR_MAX_TOTAL_ELAPSED_MS", 15000)
        ),
        recovery_attempt_number=max(1, int(recovery_attempt_number or 1)),
        recovery_cursor=dict(recovery_cursor or {}) if recovery_cursor else None,
        recovery_cursor_persist=recovery_cursor_persist,
        affordability_headroom_pct=_bounded_float_env(
            "SELECTOR_RECOVERY_AFFORDABILITY_HEADROOM_PCT", 0.10, 0.0, 0.25
        ),
        symbol_refresh_seconds=max(
            5,
            min(
                120,
                _positive_int_env("SELECTOR_RECOVERY_SYMBOL_REFRESH_SECONDS", 20),
            ),
        ),
    )
    if context.recovery_cursor:
        try:
            from ap.selector_retry_policy import selector_symbol_may_retry
            for symbol, record in dict(
                context.recovery_cursor.get("attempted_symbols") or {}
            ).items():
                if not selector_symbol_may_retry(
                    record,
                    refresh_seconds=context.symbol_refresh_seconds,
                ):
                    context.revalidated_contracts.add(
                        "".join(str(symbol or "").upper().split())
                    )
        except Exception as exc:
            log.warning("selector recovery cursor seed failed err=%s", exc)
            # Fail closed on malformed progress: never repeat a symbol merely
            # because cursor parsing failed.
            for symbol in dict(
                (context.recovery_cursor or {}).get("attempted_symbols") or {}
            ):
                context.revalidated_contracts.add(
                    "".join(str(symbol or "").upper().split())
                )
    return context


def _ctx_elapsed_ms(ctx: SelectorRequestContext | None) -> float:
    if ctx is None:
        return 0.0
    return max(
        0.0,
        (time.monotonic() - float(ctx.started_at_monotonic or time.monotonic())) * 1000.0,
    )


def _ctx_refresh_diagnostics(ctx: SelectorRequestContext | None) -> None:
    if ctx is None or not isinstance(ctx.diagnostics_sink, dict):
        return
    ctx.diagnostics_sink.update(_selector_request_diagnostics(ctx))


def _bind_selector_request_diagnostics(plan, ctx: SelectorRequestContext | None) -> None:
    if ctx is None:
        return
    try:
        if isinstance(plan, dict):
            metadata = plan.setdefault("metadata", {})
        else:
            metadata = getattr(plan, "metadata", None)
            if not isinstance(metadata, dict):
                metadata = {}
                setattr(plan, "metadata", metadata)
        if isinstance(metadata, dict):
            sink = metadata.setdefault("selector_request_diagnostics", {})
            if isinstance(sink, dict):
                ctx.diagnostics_sink = sink
                _ctx_refresh_diagnostics(ctx)
    except Exception:
        pass


def _request_playbook_enabled(ctx: SelectorRequestContext | None) -> bool:
    if ctx is None:
        return playbook_contract_selection_enabled()
    if getattr(ctx, "playbook_enabled", None) is None:
        ctx.playbook_enabled = playbook_contract_selection_enabled()
        _ctx_refresh_diagnostics(ctx)
    return bool(ctx.playbook_enabled)


def _ctx_assert_budget(
    ctx: SelectorRequestContext | None,
    *,
    stage: str,
    call_key: str | None = None,
) -> None:
    if ctx is None:
        return
    elapsed_ms = _ctx_elapsed_ms(ctx)
    if elapsed_ms >= int(ctx.max_total_elapsed_ms):
        detail = f"elapsed_ms={elapsed_ms:.3f} limit_ms={ctx.max_total_elapsed_ms}"
    else:
        limit_by_key = {
            "expiration_calls": int(ctx.max_expiration_calls),
            "chain_calls": int(ctx.max_chain_calls),
            "direct_quote_calls": int(ctx.effective_direct_quote_limit),
        }
        if call_key is None:
            return
        limit = limit_by_key[call_key]
        used = int(ctx.provider_call_counts.get(call_key, 0) or 0)
        if used < limit:
            return
        detail = f"{call_key}={used} limit={limit}"
    ctx.budget_exhausted_stage = stage
    ctx.budget_exhausted_detail = detail
    _ctx_refresh_diagnostics(ctx)
    raise SelectorRequestBudgetExhausted(stage, detail)


def _ctx_increment(ctx: SelectorRequestContext | None, key: str, count: int = 1) -> None:
    if ctx is None:
        return
    ctx.provider_call_counts[key] = int(ctx.provider_call_counts.get(key, 0) or 0) + count
    _ctx_refresh_diagnostics(ctx)


def _ctx_add_stage_ms(ctx: SelectorRequestContext | None, key: str, elapsed_ms: float) -> None:
    if ctx is None:
        return
    ctx.elapsed_ms_by_stage[key] = float(ctx.elapsed_ms_by_stage.get(key, 0.0) or 0.0) + float(elapsed_ms)
    _ctx_refresh_diagnostics(ctx)


def _ctx_add_throttle_wait(ctx: SelectorRequestContext | None, wait_ms: float) -> None:
    if ctx is None:
        return
    ctx.throttle_wait_ms += float(wait_ms or 0.0)
    _ctx_refresh_diagnostics(ctx)


def _ctx_note_throttle_issue(
    ctx: SelectorRequestContext | None,
    *,
    endpoint: str,
    symbol: str,
    context: str,
    phase: str,
    error: Exception,
) -> None:
    if ctx is None:
        return
    ctx.throttle_diagnostics.append({
        "endpoint": str(endpoint),
        "symbol": str(symbol),
        "context": str(context),
        "phase": str(phase),
        "error": str(error),
    })
    _ctx_refresh_diagnostics(ctx)


def _selector_request_diagnostics(ctx: SelectorRequestContext | None) -> dict:
    if ctx is None:
        return {}
    direct_quote_used = int(ctx.provider_call_counts.get("direct_quote_calls", 0) or 0)
    # Generic missing-context fallback is the ordinary pre-PR #401 default of
    # five. A real deferred request carries its explicit 40 on the context, so
    # this fallback never reduces deferred capacity.
    effective_direct_quote_limit = int(ctx.effective_direct_quote_limit or ctx.max_direct_quote_calls or 5)
    direct_quote_remaining = max(0, effective_direct_quote_limit - direct_quote_used)
    return {
        "underlying_quote_calls": int(ctx.provider_call_counts.get("underlying_quote_calls", 0) or 0),
        "expiration_calls": int(ctx.provider_call_counts.get("expiration_calls", 0) or 0),
        "chain_calls": int(ctx.provider_call_counts.get("chain_calls", 0) or 0),
        "direct_quote_calls": direct_quote_used,
        "provider_call_counts": dict(ctx.provider_call_counts),
        "elapsed_ms_by_stage": {
            key: round(float(value or 0.0), 3)
            for key, value in ctx.elapsed_ms_by_stage.items()
        },
        "throttle_wait_ms": round(float(ctx.throttle_wait_ms or 0.0), 3),
        "selector_elapsed_ms": round(_ctx_elapsed_ms(ctx), 3),
        "expirations_probed": list(ctx.expirations_probed),
        "throttle_diagnostics": list(ctx.throttle_diagnostics),
        "direct_quote_attempts_remaining": direct_quote_remaining,
        "direct_quote_budget": {
            "canonical_env": ctx.configured_selector_max_direct_quote_calls,
            "direct_recovery_alias": ctx.configured_direct_quote_recovery_top_n,
            "contract_revalidate_alias": ctx.configured_contract_revalidate_top_n,
            "source": str(ctx.direct_quote_budget_source or "default"),
            "effective_limit": effective_direct_quote_limit,
            "used": direct_quote_used,
            "remaining": direct_quote_remaining,
            "conflict": bool(ctx.direct_quote_budget_conflict),
            "conflict_detail": ctx.direct_quote_budget_conflict_detail,
        },
        "direct_quote_structural_candidates": int(ctx.direct_quote_structural_candidates or 0),
        "direct_quote_eligible_candidates": int(ctx.direct_quote_eligible_candidates or 0),
        "direct_quote_attempted_symbols": list(ctx.direct_quote_attempted_symbols),
        "direct_quote_unattempted_count": int(ctx.direct_quote_unattempted_count or 0),
        "direct_quote_unattempted_symbols": list(ctx.direct_quote_unattempted_symbols[:25]),
        "direct_quote_candidate_ranking": list(ctx.direct_quote_candidate_ranking[:25]),
        "direct_quote_duplicate_symbols": list(ctx.direct_quote_duplicate_symbols[:25]),
        "duplicate_quote_conflicts": list(ctx.duplicate_quote_conflicts[:25]),
        "duplicate_quote_conflict_symbols": sorted(
            str(symbol) for symbol in ctx.duplicate_quote_conflict_symbols
        )[:25],
        "duplicate_quote_conflict_dimensions": {
            str(symbol): list(dimensions)
            for symbol, dimensions in list(
                ctx.duplicate_quote_conflict_dimensions.items()
            )[:25]
        },
        "duplicate_quote_authority_symbols": sorted(
            str(symbol) for symbol in ctx.duplicate_quote_authority
        )[:25],
        "duplicate_quote_authority_failures": dict(
            list(ctx.duplicate_quote_authority_failures.items())[:25]
        ),
        "candidate_accounting": {
            "universe_count": int(ctx.selector_candidate_universe_count or 0),
            "accounted_count": int(ctx.selector_candidate_accounted_count or 0),
            "complete": bool(ctx.selector_candidate_accounting_complete),
        },
        "candidate_outcomes": dict(
            list(ctx.selector_candidate_outcomes.items())[-200:]
        ),
        "selector_request_kind": str(ctx.selector_request_kind or SELECTOR_REQUEST_KIND_ORDINARY),
        "recovery_attempt_number": int(ctx.recovery_attempt_number or 1),
        "structural_skips": list(ctx.structural_skips[:200]),
        "limits": {
            "max_expiration_calls": int(ctx.max_expiration_calls),
            "max_chain_calls": int(ctx.max_chain_calls),
            "max_direct_quote_calls": effective_direct_quote_limit,
            "max_total_elapsed_ms": int(ctx.max_total_elapsed_ms),
        },
        "budget_exhausted_stage": ctx.budget_exhausted_stage,
        "budget_exhausted_detail": ctx.budget_exhausted_detail,
        "legacy_fallback_used": bool(ctx.legacy_fallback_used),
        "execution_mode": str(ctx.execution_mode or "unknown"),
        "playbook_ordered_expirations": list(ctx.playbook_ordered_expirations or []),
        "playbook_audit": dict(ctx.playbook_audit or {}) if isinstance(ctx.playbook_audit, dict) else None,
        "playbook_enabled": ctx.playbook_enabled,
        "playbook_today_et": ctx.playbook_today_et.isoformat() if isinstance(ctx.playbook_today_et, date) else None,
    }


def _selector_operational_reason(ctx: SelectorRequestContext | None) -> str | None:
    """Return the selector-owned operational stop reason, if this request hit a cap."""
    if ctx is None:
        return None
    if getattr(ctx, "budget_exhausted_stage", None) and getattr(
        ctx, "budget_exhausted_detail", None
    ):
        return "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    return None


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

    if _rc == "CHAIN_ROW_ZERO_BID_ASK":
        # Use computed validity ratios to make an accurate classification.
        # "Mostly zero rows" = no valid rows OR zero_quote_ratio > 0.5.
        # If valid rows exist but were minority, zero quotes were not dominant
        # and the valid contracts must have failed a quality gate.
        if _both_gt == 0 or _zero_ratio > 0.5:
            return "data_quality_zero_quotes", True, False
        else:
            # Valid quotes existed but contracts still failed — quality issue.
            return "contract_quality_reject", False, True

    if _rc == "DIRECT_QUOTE_ZERO_BID_ASK":
        # Direct quote returning zero is always a data-source issue.
        return "data_quality_zero_quotes", True, False

    if _rc in ("QUOTE_ZERO_BID_ASK", "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
               "CHAIN_PROVIDER_EMPTY_OPTIONS",
               "CHAIN_PROVIDER_EMPTY_EXPIRATIONS", "NO_CHAIN_DATA",
               "CHAIN_PROVIDER_ERROR", "CHAIN_AUTH_ERROR"):
        return "data_quality_zero_quotes", True, False

    # Mixed: some valid rows exist but selector still failed — quality gate issue.
    if _both_gt > 0:
        return "contract_quality_reject", False, True

    return "data_quality_zero_quotes", True, False


# ── P0 PR #302 — Fix 1: paper domain utilities ────────────────────────────────

def _is_paper_sandbox_data_failure(reason_code: str) -> bool:
    """True when the reject reason indicates sandbox data quality, not contract quality."""
    return str(reason_code or "").upper() in _PAPER_SANDBOX_DATA_FAILURE_CODES


def _detect_paper_selector_domain(sel_base_url: str) -> str:
    """
    Classify the ACTUAL data domain from the selector's base URL.
    Returns: "live" | "sandbox" | "unknown"

    IMPORTANT: This function classifies from the actual base_url of the data
    broker — it does NOT override using PAPER_SELECTOR_MARKET_DATA_DOMAIN.
    The env var has no routing power inside contract_selector.py (routing is
    wired at client_runner.py level). Using the env var here would produce a
    label that says "live" while the actual data comes from sandbox, which is
    misleading and hides misconfiguration.

    The env var _PAPER_SELECTOR_MARKET_DATA_DOMAIN is used separately:
    - In the no-survivors path to decide if sandbox failures should be
      reclassified as PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE.
    - To emit a misconfiguration warning when env requests live but URL is sandbox.
    """
    _url = str(sel_base_url or "").lower()
    if "api.tradier.com" in _url:
        return "live"
    if "sandbox" in _url:
        return "sandbox"
    if _url:
        return "unknown"
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


def _option_expiration_date(opt: dict) -> date | None:
    raw = opt.get("expiration_date") or opt.get("expiration")
    if raw:
        try:
            return date.fromisoformat(str(raw)[:10])
        except Exception:
            pass
    symbol = "".join(str(opt.get("symbol") or opt.get("contract") or "").upper().split())
    if len(symbol) >= 15:
        try:
            return datetime.strptime(symbol[-15:-9], "%y%m%d").date()
        except Exception:
            return None
    return None


def _option_strike(opt: dict) -> float | None:
    strike = opt.get("strike")
    if strike not in (None, ""):
        try:
            return float(strike)
        except Exception:
            pass
    symbol = "".join(str(opt.get("symbol") or opt.get("contract") or "").upper().split())
    if len(symbol) >= 8:
        try:
            return int(symbol[-8:]) / 1000.0
        except Exception:
            return None
    return None


def _option_type(opt: dict) -> str:
    raw = opt.get("option_type") or opt.get("type") or opt.get("put_call")
    if raw:
        value = str(raw).strip().upper()
        if value.startswith("C"):
            return "CALL"
        if value.startswith("P"):
            return "PUT"
    symbol = "".join(str(opt.get("symbol") or opt.get("contract") or "").upper().split())
    if len(symbol) >= 9 and symbol[-9] in ("C", "P"):
        return "CALL" if symbol[-9] == "C" else "PUT"
    return ""


def _valid_occ_symbol(opt: dict) -> bool:
    symbol = "".join(str(opt.get("symbol") or opt.get("contract") or "").upper().split())
    if len(symbol) < 15:
        return False
    try:
        datetime.strptime(symbol[-15:-9], "%y%m%d")
        int(symbol[-8:])
    except Exception:
        return False
    return symbol[-9] in ("C", "P")


def _structural_direct_quote_skip(
    engine,
    opt: dict,
    *,
    direction: str,
    ticker: str,
    underlying_price: float,
    today: date,
    selector_budget: float,
    request_context: SelectorRequestContext | None,
) -> dict | None:
    """Return a diagnostic when known chain facts make a provider call futile."""
    if (
        request_context is None
        or str(request_context.selector_request_kind).strip().upper()
        != SELECTOR_REQUEST_KIND_DEFERRED_BREACH
    ):
        return None
    direction_norm = str(direction or "").strip().upper()
    if direction_norm.startswith("C"):
        direction_norm = "CALL"
    elif direction_norm.startswith("P"):
        direction_norm = "PUT"
    symbol = "".join(
        str(opt.get("symbol") or opt.get("contract") or "").upper().split()
    )
    strike = _option_strike(opt)
    exp_date = _option_expiration_date(opt)
    dte = (exp_date - today).days if exp_date else None
    delta, _ = _extract_abs_delta(opt)
    chain_bid = _safe_float(opt.get("bid"))
    chain_ask = _safe_float(opt.get("ask"))
    estimated_cost = chain_ask * 100.0 if chain_ask and chain_ask > 0 else None
    ticker_cap = _get_max_premium(ticker)
    headroom = float(
        getattr(request_context, "affordability_headroom_pct", 0.10) or 0.0
    )
    reason = None
    if not _valid_occ_symbol(opt):
        reason = "STRUCTURAL_INVALID_OCC"
    elif _option_type(opt) != direction_norm:
        reason = "STRUCTURAL_SIDE_MISMATCH"
    elif exp_date is None or dte is None or dte < int(engine.min_dte) or dte > int(engine.max_dte):
        reason = "STRUCTURAL_DTE_OUT_OF_RANGE"
    elif strike is None or not underlying_price:
        reason = "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
    else:
        max_otm_pct = _bounded_float_env("MAX_OTM_PCT", 0.12, 0.0, 1.0)
        if direction_norm == "CALL":
            otm_pct = max(0.0, (float(strike) - float(underlying_price)) / float(underlying_price))
        else:
            otm_pct = max(0.0, (float(underlying_price) - float(strike)) / float(underlying_price))
        if otm_pct > max_otm_pct:
            reason = "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
    if reason is None and delta is not None:
        min_delta = max(0.05, float(engine.target_delta) - float(engine.delta_band))
        max_delta = min(0.95, float(engine.target_delta) + float(engine.delta_band))
        if delta < min_delta or delta > max_delta:
            reason = "STRUCTURAL_DELTA_OUT_OF_RANGE"
    if reason is None and symbol and request_context is not None:
        if symbol in request_context.revalidated_contracts:
            reason = "STRUCTURAL_ALREADY_ATTEMPTED"
    # Affordability and premium-cap MUST NOT be derived from the chain ask at
    # this seam: this prefilter is only reached because the chain row already
    # failed a revalidatable quote-quality rule (zero/missing bid/ask, etc.).
    # A stale or broken chain ask cannot suppress the fresh direct quote that
    # may itself be affordable. Affordability and premium cap are authoritative
    # only against the fresh direct-quote ask, which the existing live-quote
    # quality / sizing paths evaluate after _revalidate_direct() succeeds.
    # chain_ask / estimated_contract_cost / ticker_cap remain in diagnostics
    # below but must never gate the provider call.
    if reason is None:
        return None

    diagnostic = {
        "symbol": symbol,
        "skip_reason": reason,
        "strike": strike,
        "expiration": exp_date.isoformat() if exp_date else None,
        "dte": dte,
        "delta": delta,
        "chain_bid": chain_bid,
        "chain_ask": chain_ask,
        "estimated_contract_cost": estimated_cost,
        "selector_budget": float(selector_budget or 0.0),
        "ticker_premium_cap": ticker_cap,
        "affordability_headroom_pct": headroom,
        "trigger_anchor_tier": None,
        "provider_call_consumed": False,
    }
    if request_context is not None:
        request_context.structural_skips.append(diagnostic)
        request_context.structural_skips[:] = request_context.structural_skips[-200:]
        from ap.selector_retry_policy import record_selector_structural_skip
        request_context.recovery_cursor = record_selector_structural_skip(
            request_context.recovery_cursor or {},
            symbol=symbol,
            skip_reason=reason,
        )
        _ctx_persist_structural_skip(
            request_context,
            symbol,
            structural_skip_reason=reason,
        )
        _ctx_refresh_diagnostics(request_context)
    return diagnostic


def _order_chain_for_direct_quote_recovery(
    chain: list[dict],
    *,
    direction: str,
    underlying_price: float,
    target_delta: float,
    today: date,
    request_context: SelectorRequestContext,
    preferred_strikes: list[float] | tuple[float, ...] | None = None,
) -> list[dict]:
    direction_norm = str(direction or "").strip().upper()
    if direction_norm.startswith("C"):
        direction_norm = "CALL"
    elif direction_norm.startswith("P"):
        direction_norm = "PUT"
    preferred_order: dict[float, int] = {}
    for idx, raw_strike in enumerate(preferred_strikes or ()):
        try:
            strike_value = float(raw_strike)
        except (TypeError, ValueError):
            continue
        if strike_value > 0 and strike_value not in preferred_order:
            preferred_order[strike_value] = idx

    def _preferred_tier(strike: float | None) -> tuple[int, float]:
        if not preferred_order:
            return 0, 0.0
        if strike is None:
            return len(preferred_order) + 1, float("inf")
        for preferred, tier in preferred_order.items():
            if abs(float(strike) - preferred) < 1e-9:
                return tier, 0.0
        return (
            len(preferred_order),
            min(abs(float(strike) - preferred) for preferred in preferred_order),
        )

    rows: list[tuple[tuple, dict, dict]] = []

    def _canonical_occ_symbol(opt: dict) -> str:
        return "".join(
            str(opt.get("symbol") or opt.get("contract") or "").upper().split()
        )

    def _duplicate_resolution_key(opt: dict) -> tuple:
        """Order repeated OCC rows by fields that affect selection quality.

        Duplicate rows are deliberately kept through the quality loop. For
        non-conflicting rows this key makes quality evidence deterministic: a
        usable quote wins over a zero/crossed quote, then the lower execution
        ask, tighter spread, and stronger liquidity win. A conflict group is
        resolved by normalized direct-quote authority before these fields can
        affect selection; this key is then diagnostic ordering only. Provider
        metadata is a final tie-breaker only after financial fields are equal;
        unrelated raw payload fields never decide authority.
        """
        bid = _safe_float(opt.get("bid"), 0.0)
        ask = _safe_float(opt.get("ask"), 0.0)
        quote_usable = bid > 0.0 and ask > 0.0 and ask >= bid
        spread_pct = (
            (ask - bid) / ask
            if quote_usable and ask > 0.0
            else float("inf")
        )
        provider_index = opt.get("_provider_index")
        try:
            provider_index = float(provider_index)
        except (TypeError, ValueError):
            provider_index = float("inf")
        return (
            0 if quote_usable else 1,
            ask if quote_usable else float("inf"),
            spread_pct,
            -_safe_float(opt.get("open_interest"), 0.0),
            -_safe_float(opt.get("volume"), 0.0),
            provider_index,
        )

    def _price_difference_is_decision_relevant(left: float, right: float) -> bool:
        """Require authority for any quote disagreement beyond tick rounding.

        A percentage threshold is unsafe here: the selector has hard dollar
        affordability boundaries, so a small absolute difference can change
        whether a LIVE order exists. Tradier option prices are cent-granular;
        half a cent is the only tolerance allowed for representation noise.
        """
        return abs(left - right) > 0.005

    def _duplicate_conflict_dimensions(
        group: list[tuple[tuple, dict, dict]],
    ) -> tuple[str, ...]:
        dimensions: set[str] = set()
        valid_quotes = []
        for _, item, _ in group:
            bid = _safe_float(item.get("bid"), 0.0)
            ask = _safe_float(item.get("ask"), 0.0)
            if bid > 0.0 and ask > 0.0 and ask >= bid:
                valid_quotes.append((bid, ask))
        for index, (bid, ask) in enumerate(valid_quotes):
            for other_bid, other_ask in valid_quotes[index + 1:]:
                if (
                    _price_difference_is_decision_relevant(bid, other_bid)
                    or _price_difference_is_decision_relevant(ask, other_ask)
                ):
                    dimensions.add("price")
                    break
            if "price" in dimensions:
                break

        def _numeric_representation(item: dict, key: str):
            raw_value = item.get(key)
            if raw_value in (None, ""):
                return None
            try:
                return round(float(raw_value), 8)
            except (TypeError, ValueError):
                return "INVALID"

        for liquidity_key in ("open_interest", "volume"):
            values = {
                _numeric_representation(item, liquidity_key)
                for _, item, _ in group
            }
            if len(values) > 1:
                dimensions.add(liquidity_key)

        # Delta is both a hard eligibility gate and a ranking input. The same
        # OCC may not inherit authority from whichever duplicate happens to
        # carry the favorable Greek. Normalize exactly as the selector does so
        # CALL/PUT sign does not create a false conflict, while missing/dirty
        # versus valid and materially different deltas do.
        normalized_deltas = {
            (
                round(float(_extract_abs_delta(item)[0]), 8)
                if _extract_abs_delta(item)[0] is not None
                else None
            )
            for _, item, _ in group
        }
        if len(normalized_deltas) > 1:
            dimensions.add("delta")
        return tuple(
            dimension
            for dimension in ("price", "open_interest", "volume", "delta")
            if dimension in dimensions
        )

    for original_index, opt in enumerate(list(chain or [])):
        exp_date = _option_expiration_date(opt)
        strike = _option_strike(opt)
        opt_type = _option_type(opt)
        dte = (exp_date - today).days if exp_date else None
        abs_delta, _ = _extract_abs_delta(opt)
        delta_distance = (
            abs(abs_delta - abs(float(target_delta or 0.0)))
            if abs_delta is not None and target_delta is not None
            else None
        )
        strike_distance = (
            abs(float(strike) - float(underlying_price))
            if strike is not None and underlying_price
            else None
        )
        moneyness_distance_pct = (
            strike_distance / abs(float(underlying_price))
            if strike_distance is not None and underlying_price
            else None
        )
        valid_expiration = exp_date is not None and (dte is None or dte >= 0)
        valid_option_side = opt_type == direction_norm
        if strike is None or not underlying_price or not valid_option_side:
            directional_fit = False
        elif direction_norm == "CALL":
            directional_fit = strike >= float(underlying_price)
        elif direction_norm == "PUT":
            directional_fit = strike <= float(underlying_price)
        else:
            directional_fit = False
        preferred_strike_tier, preferred_strike_distance = _preferred_tier(strike)
        oi_missing = opt.get("open_interest") in (None, "")
        vol_missing = opt.get("volume") in (None, "")
        open_interest = int(_safe_float(opt.get("open_interest"), 0.0)) if not oi_missing else None
        volume = int(_safe_float(opt.get("volume"), 0.0)) if not vol_missing else None
        canonical_symbol = _canonical_occ_symbol(opt)
        ranking = {
            "rank": 0,
            "symbol": opt.get("symbol") or opt.get("contract"),
            "strike": strike,
            "expiration": exp_date.isoformat() if exp_date else None,
            "dte": dte,
            "delta": abs_delta,
            "delta_distance": delta_distance,
            "moneyness_distance_pct": moneyness_distance_pct,
            "valid_option_side": valid_option_side,
            "directional_strike_fit": directional_fit,
            "preferred_strike_tier": preferred_strike_tier,
            "preferred_strike_distance": preferred_strike_distance,
            "open_interest": open_interest,
            "volume": volume,
            "original_index": original_index,
        }
        # Raw provider order is not a stable tie-break authority. The same
        # candidate set can arrive in a different insertion order after a
        # restart or provider pagination; equal-ranked OCC identities are
        # resolved by the explicit duplicate-quality pass below. Keep
        # original_index as diagnostic evidence only.
        stable_tie_key = (
            canonical_symbol,
            exp_date.isoformat() if exp_date else "",
            opt_type,
            float(strike) if strike is not None else float("inf"),
            float(abs_delta) if abs_delta is not None else float("inf"),
        )
        sort_key = (
            0 if _valid_occ_symbol(opt) else 1,
            0 if valid_expiration else 1,
            0 if valid_option_side else 1,
            preferred_strike_tier,
            preferred_strike_distance,
            0 if directional_fit else 1,
            strike_distance if strike_distance is not None else float("inf"),
            delta_distance if delta_distance is not None else float("inf"),
            -(open_interest or 0),
            -(volume or 0),
            stable_tie_key,
        )
        rows.append((sort_key, opt, ranking))
    rows.sort(key=lambda item: item[0])

    # Tradier normally returns one row per OCC contract, but merged/paginated
    # payloads and replay fixtures can repeat the same normalized symbol. Do
    # not discard a duplicate before quality evaluation: one row may be stale
    # or zero-quoted while another carries the usable chain quote. Resolve the
    # duplicate group by real quote/quality fields, then let the quality loop
    # inspect every row. ``revalidate_with_direct_quote`` independently fences
    # actual provider calls with request_context.revalidated_contracts, so a
    # repeated OCC can never consume the direct-quote budget twice. Build the
    # groups by identity first, then flatten each group at its earliest ranked
    # position. This does not rely on duplicate rows being adjacent: a distinct
    # same-strike candidate with different liquidity can otherwise interleave
    # two representations and bypass the financial duplicate-resolution key.
    duplicate_groups: dict[str, list[tuple[tuple, dict, dict]]] = {}
    for item in rows:
        opt = item[1]
        canonical_symbol = _canonical_occ_symbol(opt)
        if _valid_occ_symbol(opt) and canonical_symbol:
            duplicate_groups.setdefault(canonical_symbol, []).append(item)

    resolved_rows: list[tuple[tuple, dict, dict]] = []
    duplicate_symbols: list[str] = []
    emitted_symbols: set[str] = set()
    for item in rows:
        opt = item[1]
        canonical_symbol = _canonical_occ_symbol(opt)
        if not (_valid_occ_symbol(opt) and canonical_symbol):
            resolved_rows.append(item)
            continue
        if canonical_symbol in emitted_symbols:
            continue
        group = duplicate_groups[canonical_symbol]
        if len(group) > 1:
            _conflict_dimensions = _duplicate_conflict_dimensions(group)
            if _conflict_dimensions and request_context is not None:
                request_context.duplicate_quote_conflict_dimensions[canonical_symbol] = (
                    _conflict_dimensions
                )
                if canonical_symbol not in request_context.duplicate_quote_conflict_symbols:
                    request_context.duplicate_quote_conflict_symbols.add(canonical_symbol)
                    request_context.duplicate_quote_conflicts.append({
                        "symbol": canonical_symbol,
                        "authority": "DIRECT_QUOTE_NORMALIZED_OCC",
                        "reason": "DUPLICATE_QUOTE_CONFLICT",
                        "dimensions": list(_conflict_dimensions),
                        "representations": sorted(
                            [
                                {
                                    "bid": _safe_float(group_item[1].get("bid"), 0.0),
                                    "ask": _safe_float(group_item[1].get("ask"), 0.0),
                                    "open_interest": int(
                                        _safe_float(group_item[1].get("open_interest"), 0.0)
                                    ),
                                    "volume": int(
                                        _safe_float(group_item[1].get("volume"), 0.0)
                                    ),
                                    "delta": _extract_abs_delta(group_item[1])[0],
                                }
                                for group_item in group
                            ],
                            key=lambda representation: (
                                representation["bid"],
                                representation["ask"],
                                -representation["open_interest"],
                                -representation["volume"],
                                (
                                    representation["delta"]
                                    if representation["delta"] is not None
                                    else float("inf")
                                ),
                            ),
                        ),
                    })
                else:
                    for conflict in request_context.duplicate_quote_conflicts:
                        if conflict.get("symbol") == canonical_symbol:
                            conflict["dimensions"] = list(_conflict_dimensions)
                            break
            group = sorted(
                group,
                key=lambda group_item: _duplicate_resolution_key(group_item[1]),
            )
            duplicate_symbols.append(canonical_symbol)
        resolved_rows.extend(group)
        emitted_symbols.add(canonical_symbol)
    rows = resolved_rows

    rankings = []
    for rank, (_, _, ranking) in enumerate(rows, start=1):
        ranked = dict(ranking)
        ranked["rank"] = rank
        rankings.append(ranked)
    if request_context is not None:
        # ``direct_quote_structural_candidates`` counts rows that are at
        # least structurally direct-quotable — valid OCC symbol, valid
        # expiration, and the requested CALL/PUT side. Current-underlying
        # moneyness is a ranking preference only: a slightly ITM
        # trigger-primary contract remains structurally quotable. Actual
        # direct-quote eligibility depends on the chain reject reason being
        # one that ``_should_revalidate(...)`` accepts, which is not known
        # here and gets counted in the quality-filter loop.
        structural_rows = sum(
            1
            for sort_key, _, _ in rows
            if sort_key[0] == 0
            and sort_key[1] == 0
            and sort_key[2] == 0
        )
        request_context.direct_quote_structural_candidates = structural_rows
        request_context.direct_quote_candidate_ranking = rankings
        request_context.direct_quote_duplicate_symbols = duplicate_symbols
        _ctx_refresh_diagnostics(request_context)
    return [opt for _, opt, _ in rows]


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
        "playbook_strike_policy_tier": opt.get("_playbook_strike_policy_tier"),
        "playbook_strike_policy_label": opt.get("_playbook_strike_policy_label"),
        "playbook_strike_match": bool(opt.get("_playbook_strike_match")) if "_playbook_strike_match" in opt else None,
        "playbook_distance_to_preferred": _round_or_none(opt.get("_playbook_distance_to_preferred")),
        "playbook_original_rank_score": _round_or_none(opt.get("_playbook_original_rank_score")),
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
        if isinstance(plan, dict):
            return plan.get(attr, default)
    except Exception:
        return default
    try:
        return getattr(plan, attr, default)
    except Exception:
        return default


def _persist_dte_ladder_audit(selector, plan, audit: dict) -> None:
    """Persist request evidence both on the selector and the request plan."""
    snapshot = dict(audit)
    if hasattr(selector, "_set_last_dte_ladder_audit"):
        selector._set_last_dte_ladder_audit(snapshot)
    else:
        selector._last_dte_ladder_audit = snapshot
    try:
        if isinstance(plan, dict):
            metadata = plan.setdefault("metadata", {})
        else:
            metadata = getattr(plan, "metadata", None)
            if not isinstance(metadata, dict):
                metadata = {}
                setattr(plan, "metadata", metadata)
        if isinstance(metadata, dict):
            metadata["dte_ladder_audit"] = snapshot
    except Exception:
        pass


def _retry_after_ms(response) -> int | None:
    """Return numeric Retry-After seconds as milliseconds when supplied."""
    try:
        raw = (getattr(response, "headers", None) or {}).get("Retry-After")
        if raw is None or str(raw).strip() == "":
            return None
        return max(0, int(float(str(raw).strip()) * 1000))
    except (TypeError, ValueError, AttributeError):
        return None


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

# P0 PR #302 compatibility helper retained for older tests/callers. It no
# longer owns per-request direct quote budget authority.
def _safe_int_env(primary: str, fallback: str, default: int) -> int:
    """Parse int from env var safely. Logs warning on parse failure, never raises."""
    import logging as _log_env
    for _key in (primary, fallback):
        _raw = (os.getenv(_key) or "").strip()
        if _raw:
            try:
                return max(1, int(_raw))
            except (ValueError, TypeError):
                _log_env.getLogger("ap.contract_selector").warning(
                    "SELECTOR_ENV_PARSE_ERROR key=%s value=%r expected_type=int "
                    "— ignoring malformed value, using default=%d",
                    _key, _raw, default,
                )
    return default

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
_REQUEST_STATE_UNSET = object()

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

def _normalized_selector_mode(mode: str) -> str:
    return str(mode or "").strip().upper()

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
    mode = _normalized_selector_mode(mode)
    is_live = mode == "LIVE"
    return is_live, "ASK_EXECUTION" if is_live else "MID_SIMULATION"

def _selector_budget_constraints(plan) -> tuple[float, dict]:
    """Return the effective selector capital constraint and its diagnostics.

    Production sizing metadata carries multiple capital limits. The selector
    must rank and size against the tightest real constraint, not the generic
    theoretical risk budget.
    """
    authoritative_keys = (
        "selector_budget",
        "remaining_capacity",
        "max_position_usd",
    )
    generic_keys = ("budget",)
    raw_values = {
        "selector_budget": _sizing_val(plan, "selector_budget", default=None),
        "remaining_capacity": _sizing_val(plan, "remaining_capacity", default=None),
        "max_position_usd": _sizing_val(plan, "max_position_usd", default=None),
        "budget": _sizing_val(plan, "budget", default=None),
    }
    positive = {}
    zero = {}
    invalid = {}
    for key, raw in raw_values.items():
        if raw is None or raw == "":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            invalid[key] = raw
            continue
        if value < 0:
            invalid[key] = raw
        elif value == 0:
            zero[key] = value
        else:
            positive[key] = value

    invalid_authoritative = {
        key: invalid[key] for key in authoritative_keys if key in invalid
    }
    if invalid_authoritative:
        return 0.0, {
            "raw": raw_values,
            "positive": positive,
            "zero": zero,
            "invalid": invalid,
            "selected_source": None,
            "selected_budget": 0.0,
            "most_restrictive": True,
            "reason_code": "INVALID_POSITION_BUDGET",
        }

    zero_authoritative = {
        key: zero[key] for key in authoritative_keys if key in zero
    }
    if zero_authoritative:
        selected_source = min(authoritative_keys, key=lambda key: 0 if key in zero_authoritative else 1)
        return 0.0, {
            "raw": raw_values,
            "positive": positive,
            "zero": zero,
            "invalid": invalid,
            "selected_source": selected_source,
            "selected_budget": 0.0,
            "most_restrictive": True,
            "reason_code": "CAPITAL_NO_REMAINING",
        }

    candidate_positive = {
        key: value
        for key, value in positive.items()
        if key in authoritative_keys or key in generic_keys
    }
    if not candidate_positive:
        return 0.0, {
            "raw": raw_values,
            "positive": positive,
            "zero": zero,
            "invalid": invalid,
            "selected_source": None,
            "selected_budget": 0.0,
            "most_restrictive": True,
            "reason_code": "INVALID_POSITION_BUDGET" if invalid else "CAPITAL_NO_REMAINING",
        }

    selected_source, selected_budget = min(candidate_positive.items(), key=lambda item: item[1])
    return selected_budget, {
        "raw": raw_values,
        "positive": candidate_positive,
        "zero": zero,
        "invalid": invalid,
        "selected_source": selected_source,
        "selected_budget": selected_budget,
        "most_restrictive": True,
        "reason_code": None,
    }

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


def _pro_recovery_candidate_reason(
    *,
    quality_reason: str,
    recovery_action: str | None,
    recovery_reason: str | None,
    fresh_quote_recovered: bool,
) -> str:
    """Choose candidate authority after the PRO-quality recovery branch.

    A quality reject observed before direct recovery is only provisional.  If
    recovery did not produce an authoritative fresh quote, its disposition is
    the candidate's truth; otherwise the post-recovery PRO-quality result is
    authoritative.  Structural/off-hours/non-revalidatable skips did not
    attempt a quote and therefore retain the original quality/structural
    reason.
    """
    _action = str(recovery_action or "").strip().upper()
    _quality = str(quality_reason or "")
    _recovery = str(recovery_reason or "")
    _no_quote_attempt = {
        "SKIP_STRUCTURAL",
        "SKIP_NOT_MARKET_HOURS",
        "SKIP_NOT_REVALIDATABLE",
        "SKIP_ALREADY_REVALIDATED",
    }
    if not _action or _action in _no_quote_attempt:
        return _recovery if _action == "SKIP_STRUCTURAL" and _recovery else _quality
    if fresh_quote_recovered:
        return _quality
    # Any recovery disposition that reached this point without authoritative
    # quote evidence owns the candidate outcome, including unavailable,
    # direct-zero, budget, and malformed/partial PASS results.
    return _recovery or "UNKNOWN_SELECTOR_RECOVERY_FAILURE"


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
        self.mode           = _normalized_selector_mode(mode)
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

        # The ladder is opt-in for the post-close rollout. Set
        # DEFERRED_DTE_LADDER=1 to enable and DEFERRED_DTE_LADDER=0 as the kill
        # switch. Normal selectors remain outside this deferred-only path.
        self.dte_ladder_enabled = os.getenv("DEFERRED_DTE_LADDER", "0").strip().lower() in ("1", "true", "yes")
        # A failed authoritative expiration fetch may invoke the legacy selector
        # once in PAPER. LIVE requires this explicit break-glass override.
        self.deferred_dte_legacy_fallback = os.getenv(
            "DEFERRED_DTE_LEGACY_FALLBACK", "0"
        ).strip().lower() in ("1", "true", "yes")
        # Bucket boundaries (inclusive upper, DTE). A=near, B=adjacent, C=fallback.
        self.dte_bucket_a_max = int(os.getenv("DTE_BUCKET_A_MAX", "2"))   # 0–2 DTE
        self.dte_bucket_b_max = int(os.getenv("DTE_BUCKET_B_MAX", "7"))   # 3–7 DTE
        # Max expirations to probe per bucket (bounds Tradier calls per breach).
        self.dte_ladder_probe_per_bucket = int(os.getenv("DTE_LADDER_PROBE_PER_BUCKET", "2"))
        self.playbook_contract_selection_enabled = playbook_contract_selection_enabled()
        # Records the last ladder run for diagnostics (observability only).
        self._last_dte_ladder_audit: Optional[dict] = None
        self._last_dte_ladder_audit_ctx = contextvars.ContextVar(
            f"selector_last_dte_ladder_audit_{id(self)}",
            default=_REQUEST_STATE_UNSET,
        )

        # PR #149 — Selector Reason Honesty.
        # Records the most recent REJECT event emitted during a single select()
        # call so the queue can surface the actual blocker (e.g. OI_TOO_LOW)
        # instead of the umbrella label "no_contract_found".
        #   Shape: {"stage": str, "reason_code": str, "explanation": str} | None
        # Reset to None at the top of every select() invocation.
        # Never affects selection — observability only.
        self._last_failure: Optional[dict] = None
        self._last_failure_ctx = contextvars.ContextVar(
            f"selector_last_failure_{id(self)}",
            default=_REQUEST_STATE_UNSET,
        )

        self.strategy_version = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
        self.git_commit       = get_git_commit()
        self.config_hash = make_config_hash({
            "mode":                   self.mode,
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
            "max_trade_usd":          _max_trade_usd(),
            "max_ideal_premium":      float(os.getenv("MAX_IDEAL_PREMIUM", "3.50")),
            "ticker_premium_caps":    TICKER_MAX_PREMIUM_PER_CONTRACT,
            "playbook_contract_selection_enabled": self.playbook_contract_selection_enabled,
        })

        log.info(
            "APContractSelectionEngine | mode=%s delta=%.2f±%.2f "
            "max_spread=%d%% dte=[%d,%d] premium=[$%.0f,$%.0f] "
            "earnings_guard=%s iv_filter=%s",
            self.mode, target_delta, delta_band,
            int(max_spread_pct * 100), min_dte, max_dte,
            min_premium, max_premium,
            type(earnings_guard).__name__ if earnings_guard is not None else "None",
            type(iv_filter).__name__ if iv_filter is not None else "None",
        )


    def _set_last_failure(self, failure: Optional[dict]) -> None:
        snapshot = dict(failure) if isinstance(failure, dict) else None
        self._last_failure = snapshot
        try:
            self._last_failure_ctx.set(snapshot)
        except Exception:
            pass

    def _set_last_dte_ladder_audit(self, audit: Optional[dict]) -> None:
        snapshot = dict(audit) if isinstance(audit, dict) else None
        self._last_dte_ladder_audit = snapshot
        try:
            self._last_dte_ladder_audit_ctx.set(snapshot)
        except Exception:
            pass

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
                self._set_last_failure({
                    "stage":       str(stage or ""),
                    "reason_code": str(reason_code or "") or "UNKNOWN_REJECTION",
                    "explanation": str(explanation or ""),
                })
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
                symbol=_safe_plan_attr(plan, "ticker", None),
                contract=contract,
                setup_type=_safe_plan_attr(plan, "pattern", None),
                timeframe=_safe_plan_attr(plan, "timeframe", None),
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
            ctx = getattr(self, "_last_failure_ctx", None)
            current = ctx.get() if ctx is not None else _REQUEST_STATE_UNSET
            if current is _REQUEST_STATE_UNSET:
                current = getattr(self, "_last_failure", None)
            if isinstance(current, dict):
                # Defensive copy so callers can't mutate selector state.
                return dict(current)
        except Exception:
            pass
        return None

    def get_last_dte_ladder_audit(self) -> Optional[dict]:
        """Return the audit from the most recent DTE-ladder run (buckets tried,
        survivors per bucket, selected bucket/DTE/expiration), or None if the
        ladder was not used. Observability only. Never raises."""
        try:
            ctx = getattr(self, "_last_dte_ladder_audit_ctx", None)
            current = ctx.get() if ctx is not None else _REQUEST_STATE_UNSET
            if current is _REQUEST_STATE_UNSET:
                current = getattr(self, "_last_dte_ladder_audit", None)
            if isinstance(current, dict):
                return dict(current)
        except Exception:
            pass
        return None


    def select(
        self,
        plan,
        *,
        expiration_override: Optional[str] = None,
        _dte_legacy_fallback: bool = False,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> Optional[SelectedContract]:
        # PR #149 — Selector Reason Honesty.
        # Reset failure capture at the start of every invocation so that
        # get_last_failure() reflects ONLY this call's most recent REJECT,
        # never a stale value from a prior select(). Observability only.
        self._set_last_failure(None)
        _clear_selector_failure(plan)
        if expiration_override is None and not _dte_legacy_fallback:
            self._set_last_dte_ladder_audit(None)
            try:
                _meta = plan.get("metadata") if isinstance(plan, dict) else getattr(plan, "metadata", None)
                if isinstance(_meta, dict):
                    _meta.pop("dte_ladder_audit", None)
            except Exception:
                pass
        if request_context is None:
            request_context = _new_selector_request_context(
                _safe_plan_attr(plan, "ticker"),
                self.mode,
            )
        _bind_selector_request_diagnostics(plan, request_context)
        _request_playbook_enabled(request_context)

        _selector_mode = _normalized_selector_mode(self.mode)
        _plan_mode = _normalized_selector_mode(
            _safe_plan_attr(plan, "execution_mode", "")
            or _safe_plan_attr(plan, "mode", "")
        )
        _original_client_id = _safe_plan_attr(plan, "client_id", None)
        _original_execution_mode = (
            _safe_plan_attr(plan, "execution_mode", None)
            or _safe_plan_attr(plan, "mode", None)
        )
        _original_signal_id = _safe_plan_attr(plan, "signal_id", None)

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
        _sel_mode: str = _selector_mode.lower() if _selector_mode in {"LIVE", "PAPER"} else "unknown"

        ticker    = _safe_plan_attr(plan, "ticker")
        direction = str(_safe_plan_attr(plan, "side", "") or "").upper()
        budget, _budget_constraints = _selector_budget_constraints(plan)
        budget_raw = _budget_constraints.get("selected_budget")
        _sizing_ctx = _plan_sizing_ctx(plan)
        _top_level_budget = _safe_plan_attr(plan, "max_position_usd", None)
        _budget_conflict = (
            len({
                round(float(value), 8)
                for value in (_budget_constraints.get("positive") or {}).values()
            }) > 1
        )

        try:
            _ctx_assert_budget(request_context, stage="selector_entry")
        except SelectorRequestBudgetExhausted as exc:
            _expl = str(exc)
            self._emit_selector_event(
                plan,
                stage="selector_request_budget",
                decision="REJECT",
                reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                explanation=_expl,
                context=_selector_request_diagnostics(request_context),
            )
            _attach_selector_failure(
                plan,
                reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                operational_reason="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                explanation=_expl,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                selection_diagnostics=_selector_request_diagnostics(request_context),
            )
            return None

        if _selector_mode not in {"LIVE", "PAPER"} or _plan_mode not in {"LIVE", "PAPER"}:
            _expl = (
                f"Invalid selector execution mode: selector={self.mode!r} "
                f"plan={_original_execution_mode!r}"
            )
            _mode_diag = dict(
                _selector_request_diagnostics(request_context),
                selector_mode=_selector_mode,
                plan_execution_mode=_plan_mode,
                client_id=_original_client_id,
                signal_id=_original_signal_id,
            )
            self._emit_selector_event(
                plan,
                stage="selector_entry",
                decision="REJECT",
                reason_code="INVALID_EXECUTION_MODE",
                explanation=_expl,
                inputs={
                    "selector_mode": _selector_mode,
                    "plan_execution_mode": _plan_mode,
                    "client_id": _original_client_id,
                    "signal_id": _original_signal_id,
                },
            )
            _attach_selector_failure(
                plan,
                reason_code="INVALID_EXECUTION_MODE",
                explanation=_expl,
                base_url=_sel_base_url,
                execution_mode="unknown",
                selection_diagnostics=_mode_diag,
            )
            return None

        if _selector_mode != _plan_mode:
            _expl = (
                f"Selector execution mode {_selector_mode} does not match "
                f"plan execution mode {_plan_mode}"
            )
            _mode_diag = dict(
                _selector_request_diagnostics(request_context),
                selector_mode=_selector_mode,
                plan_execution_mode=_plan_mode,
                client_id=_original_client_id,
                signal_id=_original_signal_id,
            )
            self._emit_selector_event(
                plan,
                stage="selector_entry",
                decision="REJECT",
                reason_code="EXECUTION_MODE_MISMATCH",
                explanation=_expl,
                inputs={
                    "selector_mode": _selector_mode,
                    "plan_execution_mode": _plan_mode,
                    "client_id": _original_client_id,
                    "signal_id": _original_signal_id,
                },
            )
            _attach_selector_failure(
                plan,
                reason_code="EXECUTION_MODE_MISMATCH",
                explanation=_expl,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                selection_diagnostics=_mode_diag,
            )
            return None

        if budget <= 0:
            _budget_reason = _budget_constraints.get("reason_code") or "INVALID_POSITION_BUDGET"
            if _budget_reason == "CAPITAL_NO_REMAINING":
                _expl = f"No selector capital remaining: {_budget_constraints.get('raw')!r}"
            else:
                _expl = f"Invalid selector position budget: {_budget_constraints.get('raw')!r}"
            self._emit_selector_event(
                plan,
                stage="selector_entry",
                decision="REJECT",
                reason_code=_budget_reason,
                explanation=_expl,
                inputs={
                    "budget": budget_raw,
                    "budget_constraints": _budget_constraints,
                    "sizing_context": _sizing_ctx,
                    "top_level_max_position_usd": _top_level_budget,
                },
            )
            _attach_selector_failure(
                plan,
                reason_code=_budget_reason,
                explanation=_expl,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                selection_diagnostics=_selector_request_diagnostics(request_context),
            )
            return None

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

        pt1 = _safe_plan_attr(plan, "target_underlying", None)
        wick_targets = _safe_plan_attr(plan, "wick_targets", None) or []
        _expected_move_pct = 0.0
        _wick_confidence   = 0.5
        if wick_targets:
            _expected_move_pct = float(wick_targets[0].get("distance_pct", 0) or 0)
            _wick_confidence   = float(wick_targets[0].get("confidence", 0.5) or 0.5)
        elif pt1 and pt1 > 0:
            entry_approx = _safe_plan_attr(plan, "trigger_price", pt1) or pt1
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
            and not _dte_legacy_fallback
            and self._is_ladder_eligible(plan)
        ):
            # Legacy source-guard anchor: return self._select_with_dte_ladder(plan)
            return self._select_with_dte_ladder(plan, request_context=request_context)

        # ── A. FETCH CHAIN ────────────────────────────────────────────────────
        try:
            chain, underlying_price = self._fetch_chain_with_price(
                ticker,
                direction,
                expiration_override=expiration_override,
                request_context=request_context,
            )
        except SelectorRequestBudgetExhausted as e:
            _expl = str(e)
            self._emit_selector_event(
                plan,
                stage="selector_request_budget",
                decision="REJECT",
                reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                explanation=_expl,
                context=_selector_request_diagnostics(request_context),
            )
            _attach_selector_failure(
                plan,
                reason_code="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                operational_reason="SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                explanation=_expl,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                selection_diagnostics=_selector_request_diagnostics(request_context),
            )
            return None
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
            _attach_selector_failure(plan, reason_code="CHAIN_AUTH_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_EMPTY_EXPIRATIONS", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
            _attach_selector_failure(plan, reason_code="NO_EXPIRATION_IN_DTE_WINDOW", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_EMPTY_OPTIONS", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
            _attach_selector_failure(plan, reason_code="CHAIN_PROVIDER_ERROR", explanation=_expl, base_url=_sel_base_url, execution_mode=_sel_mode, selection_diagnostics=_selector_request_diagnostics(request_context))
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
                selection_diagnostics=_selector_request_diagnostics(request_context),
            )
            return None
        _sel_chain_rows = len(chain)

        if not underlying_price:
            underlying_price = _safe_plan_attr(plan, "trigger_price", None)

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
                    _sig_id = _safe_plan_attr(plan, "signal_id", "") or ""
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
                    signal_score = float(_safe_plan_attr(plan, "score", 0) or 0)
                    min_score    = float(iv_result.get("momentum_min_score", 65.0))
                    mom_ok       = signal_score >= min_score
                    _sig_id      = _safe_plan_attr(plan, "signal_id", "") or ""
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

        # P0 PR #302 Fix 1: detect effective paper selector data domain from actual URL.
        # The domain reflects the REAL data source — not the env var preference.
        _is_paper_mode    = str(_sel_mode or "").lower() in ("paper",)
        _paper_sel_domain = _detect_paper_selector_domain(_sel_base_url)  # actual URL only
        _is_sandbox_data  = _paper_sel_domain == "sandbox"
        # Detect misconfiguration: env requests live data but actual broker is sandbox.
        if (_is_paper_mode and _is_sandbox_data
                and _PAPER_SELECTOR_MARKET_DATA_DOMAIN == "live"):
            log.warning(
                "[%s] PAPER_SELECTOR_DOMAIN_MISCONFIGURED — "
                "PAPER_SELECTOR_MARKET_DATA_DOMAIN=live but actual selector "
                "data broker URL is sandbox (%s). Live market data is not "
                "being used. Set TRADIER_MARKET_DATA_TOKEN and "
                "TRADIER_MARKET_DATA_BASE_URL to route selector to live data.",
                ticker, _sel_base_url,
            )

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
        today = getattr(request_context, "playbook_today_et", None) or date.today()

        # ── PR #396 amendment (post-review) ───────────────────────────────────
        # Align bounded direct-quote recovery spending with the playbook's
        # trigger-anchored strike preference.
        #
        # Problem this fixes: when the underlying drifts between scanner
        # qualification and breach, chain iteration order (which drives which
        # rows get to spend SELECTOR_MAX_DIRECT_QUOTE_CALLS) can favor strikes
        # clustered around the *current* underlying while the playbook's final
        # ranker (build_playbook_candidate_context) later prefers strikes
        # anchored to the *scanner trigger*. If the trigger-preferred strike
        # never gets a fresh direct quote, it is silently discarded before
        # ranking runs. BAC production replay: trigger 61.17, underlying 61.60,
        # budget=1 — the 61 strike went unquoted and was rejected in favor of a
        # 62-cluster contract.
        #
        # Fix: gate on the playbook flag. When enabled, delegate to the single
        # shared authority (resolve_trigger_anchored_preferred_strikes) so
        # quote-spending order and final ranking cannot ever diverge. Preserve
        # existing deterministic secondary ordering for candidates in the same
        # strike tier (original chain index). When the flag is off, or trigger
        # and underlying are both unusable, fall through unchanged — the
        # non-playbook selector path preserves prior chain ordering, ranking,
        # quote spending, and selection behavior. Note: a
        # preferred_strike_ordering entry with enabled=False is still emitted
        # into selector_request_diagnostics for observability parity across
        # flag states; downstream selection is not affected.
        _preferred_strike_audit: dict = {
            "enabled": False,
            "request_kind": str(
                getattr(
                    request_context,
                    "selector_request_kind",
                    SELECTOR_REQUEST_KIND_ORDINARY,
                )
            ),
            "anchor_source": TRIGGER_ANCHOR_SOURCE_NONE,
            "anchor_price": None,
            "primary_strike": None,
            "adjacent_otm_strike": None,
            "chain_size_before": len(chain),
            "reordered": False,
        }
        _preferred_strikes_for_quality_order: tuple[float, ...] = ()
        _preferred_primary: float | None = None
        _preferred_adjacent: float | None = None
        _deferred_recovery_request = (
            str(
                getattr(
                    request_context,
                    "selector_request_kind",
                    SELECTOR_REQUEST_KIND_ORDINARY,
                )
            ).upper()
            == SELECTOR_REQUEST_KIND_DEFERRED_BREACH
        )
        if _request_playbook_enabled(request_context) or _deferred_recovery_request:
            _preferred_strike_audit["enabled"] = True
            _preference = resolve_trigger_anchored_preferred_strikes(
                side=direction,
                trigger_price=_safe_plan_attr(plan, "trigger_price", None),
                underlying_fallback=underlying_price,
                candidate_strikes=[opt.get("strike") for opt in chain],
            )
            _preferred_strike_audit["anchor_source"] = _preference.anchor_source
            _preferred_strike_audit["anchor_price"] = _preference.anchor_price
            _preferred_strike_audit["primary_strike"] = _preference.primary_strike
            _preferred_strike_audit["adjacent_otm_strike"] = _preference.adjacent_otm_strike
            if _preference.primary_strike is not None:
                _preferred_primary = float(_preference.primary_strike)
                _preferred_adjacent = (
                    float(_preference.adjacent_otm_strike)
                    if _preference.adjacent_otm_strike is not None
                    else None
                )
                _preferred_strikes_for_quality_order = tuple(
                    float(_s) for _s in (_preference.ordered_preferred_strikes or ())
                )

        survivors  = []
        _rejections: dict = {}
        _recovery_candidate_outcomes: dict[str, str] = {}
        _pro_tiers:  dict = {"A": 0, "B": 0}
        _quality_chain = _order_chain_for_direct_quote_recovery(
            chain,
            direction=direction,
            underlying_price=float(underlying_price or 0.0),
            target_delta=float(self.target_delta or 0.0),
            today=today,
            request_context=request_context,
            preferred_strikes=_preferred_strikes_for_quality_order,
        )

        def _recovery_candidate_symbol(_opt: dict) -> str:
            return "".join(
                str((_opt or {}).get("symbol") or (_opt or {}).get("contract") or "")
                .upper()
                .split()
            )

        def _record_recovery_candidate_outcome(_opt: dict, _reason: str) -> None:
            if not _deferred_recovery_request or not _reason:
                return
            _symbol = _recovery_candidate_symbol(_opt)
            if not _symbol:
                return
            _recovery_candidate_outcomes[_symbol] = _normalize_reason_code(_reason)

        _recovery_candidate_universe: set[str] = set()
        _recovery_candidate_universe_has_missing_identity = False
        for _candidate in _quality_chain:
            _candidate_symbol = _recovery_candidate_symbol(_candidate)
            if _candidate_symbol:
                _recovery_candidate_universe.add(_candidate_symbol)
            else:
                _recovery_candidate_universe_has_missing_identity = True

        if _preferred_strikes_for_quality_order:
            def _candidate_order_identity(_opt: dict, _index: int) -> str:
                _symbol = "".join(
                    str(_opt.get("symbol") or _opt.get("contract") or "")
                    .upper()
                    .split()
                )
                if _symbol:
                    return _symbol
                return "|".join(
                    (
                        str(_opt.get("expiration_date") or _opt.get("expiration") or ""),
                        str(_opt.get("option_type") or _opt.get("type") or ""),
                        str(_opt.get("strike") or ""),
                        str(_index),
                    )
                )

            _original_ids = [
                _candidate_order_identity(_opt, _index)
                for _index, _opt in enumerate(chain)
            ]
            _final_ids = [
                _candidate_order_identity(_opt, _index)
                for _index, _opt in enumerate(_quality_chain)
            ]
            _ordered_audit: list[dict] = []
            for _opt in _quality_chain:
                _s = _safe_float(_opt.get("strike"))
                if _s is None or _s <= 0:
                    _tier, _label = (
                        STRIKE_POLICY_TIER_NONMATCH,
                        STRIKE_POLICY_LABEL_NONMATCH,
                    )
                elif _preferred_primary is not None and abs(_s - _preferred_primary) < 1e-9:
                    _tier, _label = (STRIKE_POLICY_TIER_ATM, STRIKE_POLICY_LABEL_ATM)
                elif (
                    _preferred_adjacent is not None
                    and abs(_s - _preferred_adjacent) < 1e-9
                ):
                    _tier, _label = (
                        STRIKE_POLICY_TIER_ONE_STEP_OTM,
                        STRIKE_POLICY_LABEL_ONE_STEP_OTM,
                    )
                else:
                    _tier, _label = (
                        STRIKE_POLICY_TIER_NONMATCH,
                        STRIKE_POLICY_LABEL_NONMATCH,
                    )
                _ordered_audit.append({
                    "symbol": _opt.get("symbol"),
                    "strike": _safe_float(_opt.get("strike")),
                    "expiration": _opt.get("expiration_date") or _opt.get("expiration"),
                    "strike_policy_tier": _tier,
                    "strike_policy_label": _label,
                    "trigger_tier": _label,
                    "distance_to_trigger_anchor": (
                        abs(float(_s) - float(_preferred_strike_audit["anchor_price"]))
                        if _s is not None and _preferred_strike_audit.get("anchor_price")
                        else None
                    ),
                    "delta": _extract_abs_delta(_opt)[0],
                    "chain_bid": _safe_float(_opt.get("bid")),
                    "chain_ask": _safe_float(_opt.get("ask")),
                    "chain_index": int(_quality_chain.index(_opt)),
                    "final_direct_quote_rank": len(_ordered_audit) + 1,
                })
            _preferred_strike_audit["reordered"] = _original_ids != _final_ids
            _preferred_strike_audit["ordered_candidates"] = _ordered_audit
        try:
            if request_context is not None and isinstance(
                request_context.diagnostics_sink, dict
            ):
                request_context.diagnostics_sink["preferred_strike_ordering"] = dict(
                    _preferred_strike_audit
                )
                _ctx_refresh_diagnostics(request_context)
        except Exception:
            pass
        def _apply_duplicate_quote_authority(_opt: dict) -> tuple[dict, str | None]:
            """Use one direct quote for a conflicting normalized OCC group.

            Chain rows with materially different valid prices cannot establish
            LIVE/PAPER affordability by themselves. The first representation
            invokes the bounded direct-quote revalidator; every later
            representation reuses that exact normalized-OCC authority. A
            missing/blocked authority rejects the group instead of selecting
            whichever row happened to advertise the lower ask.
            """
            if request_context is None:
                return _opt, None
            _symbol = "".join(
                str(_opt.get("symbol") or _opt.get("contract") or "").upper().split()
            )
            _conflict_symbols = getattr(
                request_context, "duplicate_quote_conflict_symbols", set()
            )
            if not _symbol or _symbol not in _conflict_symbols:
                return _opt, None
            _conflict_dimensions = set(
                request_context.duplicate_quote_conflict_dimensions.get(
                    _symbol, ()
                )
            )

            _authority = request_context.duplicate_quote_authority.get(_symbol)
            if (
                _authority is None
                and _symbol not in request_context.duplicate_quote_authority_attempted
            ):
                request_context.duplicate_quote_authority_attempted.add(_symbol)
                if _symbol not in request_context.direct_quote_eligible_symbols:
                    request_context.direct_quote_eligible_symbols.add(_symbol)
                    request_context.direct_quote_eligible_candidates = len(
                        request_context.direct_quote_eligible_symbols
                    )
                _rv_duplicate = _revalidate_direct(
                    self.data_broker,
                    _opt,
                    "DUPLICATE_QUOTE_CONFLICT",
                    request_context=request_context,
                )
                _action = str(_rv_duplicate.get("action") or "")
                _rv_audit = _rv_duplicate.get("audit") or {}
                if _action == "PASS" and _rv_duplicate.get("opt_updated"):
                    _missing_authority = [
                        dimension
                        for dimension in ("open_interest", "volume", "delta")
                        if dimension in _conflict_dimensions
                        and _rv_audit.get(f"direct_{dimension}") is None
                    ]
                    if _missing_authority:
                        # A direct price without every disputed decision field
                        # is not a complete authority. Tradier's deployed
                        # get_quote adapter requests greeks=false, so a delta
                        # conflict deliberately fails closed here instead of
                        # inheriting the first/favorable duplicate Greek.
                        _authority = None
                        _failure_reason = "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
                        request_context.duplicate_quote_authority_failures[_symbol] = (
                            _failure_reason
                        )
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected": False,
                            "contract": _symbol,
                            "failure": _failure_reason,
                            "missing_authority_fields": [
                                f"direct_{dimension}"
                                for dimension in _missing_authority
                            ],
                        })
                    else:
                        _authority = dict(_rv_duplicate["opt_updated"])
                        # revalidate_with_direct_quote intentionally preserves
                        # non-zero chain liquidity for ordinary recovery. For
                        # a conflicting duplicate, direct quote values are the
                        # financial authority for both price and liquidity.
                        _direct_fields = {
                            "direct_bid": "bid",
                            "direct_ask": "ask",
                            "direct_volume": "volume",
                            "direct_open_interest": "open_interest",
                            "direct_bid_size": "bid_size",
                            "direct_ask_size": "ask_size",
                        }
                        _authoritative_fields = {"bid", "ask"}
                        for _audit_key, _opt_key in _direct_fields.items():
                            if _rv_audit.get(_audit_key) is not None:
                                _authority[_opt_key] = _rv_audit[_audit_key]
                                _authoritative_fields.add(_opt_key)
                        _authority["_duplicate_quote_authoritative_fields"] = tuple(
                            sorted(_authoritative_fields)
                        )
                        _authority["_duplicate_quote_authority"] = True
                        request_context.duplicate_quote_authority[_symbol] = _authority
                        _direct_bid = _safe_float(_rv_audit.get("direct_bid"), 0.0)
                        _direct_ask = _safe_float(_rv_audit.get("direct_ask"), 0.0)
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected": False,
                            "contract": _symbol,
                            "bid": _direct_bid,
                            "ask": _direct_ask,
                            "mid": round((_direct_bid + _direct_ask) / 2.0, 4),
                            "failure": None,
                            "duplicate_quote_authority": True,
                        })
                else:
                    if _action == "SKIP_BUDGET_EXHAUSTED":
                        _failure_reason = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
                        _direct_quote_recovery_audit["budget_skipped"] = True
                        _direct_quote_recovery_audit["budget_skip_reason"] = _failure_reason
                    elif _action == "REJECT_DIRECT_ZERO":
                        _failure_reason = "DIRECT_QUOTE_ZERO_BID_ASK"
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected": False,
                            "failure": _failure_reason,
                        })
                    elif _action == "REJECT_UNAVAILABLE":
                        _failure_reason = str(
                            _rv_duplicate.get("reason_code")
                            or "DIRECT_QUOTE_UNAVAILABLE"
                        )
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected": False,
                            "failure": _failure_reason,
                        })
                    else:
                        # Off-hours and already-revalidated states do not
                        # establish a current authority for this conflict.
                        _failure_reason = "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
                    request_context.duplicate_quote_authority_failures[_symbol] = (
                        _failure_reason
                    )
                _ctx_refresh_diagnostics(request_context)

            if _authority is not None:
                _authoritative_fields = set(
                    _authority.get("_duplicate_quote_authoritative_fields", ())
                )
                _missing_authority = [
                    dimension
                    for dimension in ("open_interest", "volume", "delta")
                    if dimension in _conflict_dimensions
                    and dimension not in _authoritative_fields
                ]
                if _missing_authority:
                    _failure_reason = "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
                    request_context.duplicate_quote_authority_failures[_symbol] = (
                        _failure_reason
                    )
                    return _opt, _failure_reason
                _patched = dict(_opt)
                for _key in (
                    "bid", "ask", "last", "volume", "open_interest",
                    "bid_size", "ask_size", "_direct_quote_used",
                    "_direct_quote_age_ms", "_direct_quote_fetch_latency_ms",
                    "_direct_quote_fetched_at", "_direct_quote_age_semantics",
                    "_chain_bid", "_chain_ask",
                ):
                    if _authority.get(_key) is not None:
                        _patched[_key] = _authority[_key]
                _patched["_duplicate_quote_authority"] = True
                return _patched, None

            return _opt, request_context.duplicate_quote_authority_failures.get(
                _symbol, "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
            )

        for opt in _quality_chain:
            _pro_recovery_action = None
            _pro_recovery_reason = None
            _pro_fresh_quote_recovered = False
            opt, _duplicate_authority_reason = _apply_duplicate_quote_authority(opt)
            if _duplicate_authority_reason:
                _rejections[_duplicate_authority_reason] = _rejections.get(
                    _duplicate_authority_reason, 0
                ) + 1
                _record_recovery_candidate_outcome(opt, _duplicate_authority_reason)
                log.warning(
                    "[%s] duplicate OCC authority unavailable symbol=%s reason=%s",
                    ticker,
                    opt.get("symbol") or opt.get("contract"),
                    _duplicate_authority_reason,
                )
                continue
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
                # and the request budget allows it, fetch a direct option quote
                # and rerun pro_quality on the patched opt before rejecting.
                # One context enforces the hard cap across every DTE probe.
                if (
                    pro_tier == "REJECT"
                    and _should_revalidate(pro_reason)
                    and not opt.get("_duplicate_quote_authority")
                ):
                    _sym_for_count = str(opt.get("symbol") or "").strip().upper()
                    if _sym_for_count and _sym_for_count not in request_context.direct_quote_eligible_symbols:
                        request_context.direct_quote_eligible_symbols.add(_sym_for_count)
                        request_context.direct_quote_eligible_candidates = len(
                            request_context.direct_quote_eligible_symbols
                        )
                    _structural_skip_pro = _structural_direct_quote_skip(
                        self,
                        opt,
                        direction=direction,
                        ticker=ticker,
                        underlying_price=float(underlying_price or 0.0),
                        today=today,
                        selector_budget=float(budget or 0.0),
                        request_context=request_context,
                    )
                    _rv_pro = (
                        {
                            "action": "SKIP_STRUCTURAL",
                            "reason_code": _structural_skip_pro.get("skip_reason"),
                            "direct_quote_used": False,
                            "opt_updated": None,
                            "audit": _structural_skip_pro,
                        }
                        if _structural_skip_pro
                        else _revalidate_direct(
                            self.data_broker,
                            opt,
                            pro_reason,
                            request_context=request_context,
                        )
                    )
                    _pro_recovery_action = _rv_pro.get("action")
                    _pro_recovery_reason = _rv_pro.get("reason_code")
                    if _rv_pro.get("action") == "PASS" and _rv_pro.get("opt_updated"):
                        _opt_pro = _rv_pro["opt_updated"]
                        _pro_fresh_quote_recovered = True
                        # Rerun pro_quality with patched bid/ask
                        pro_tier, pro_reason = _pro_contract_quality(_opt_pro, ticker, _dte)
                        # Extract direct-quote values from the revalidation audit
                        # for the audit stamp regardless of whether quality passed.
                        _rv_pro_audit   = _rv_pro.get("audit") or {}
                        _direct_bid_pro = _safe_float(_rv_pro_audit.get("direct_bid") or _opt_pro.get("bid") or 0)
                        _direct_ask_pro = _safe_float(_rv_pro_audit.get("direct_ask") or _opt_pro.get("ask") or 0)
                        _direct_mid_pro = (
                            round((_direct_bid_pro + _direct_ask_pro) / 2, 4)
                            if _direct_bid_pro and _direct_ask_pro else 0.0
                        )
                        if pro_tier != "REJECT":
                            # Pro quality passed on direct quote — use patched opt.
                            opt = _opt_pro
                            log.info(
                                "[%s] P0A pro_quality_recovered chain_reason=%s "
                                "direct_bid=%.4f direct_ask=%.4f contract=%s",
                                ticker, _rv_pro["audit"].get("chain_bid", 0),
                                _rv_pro["audit"].get("direct_bid", 0),
                                _rv_pro["audit"].get("direct_ask", 0),
                                opt.get("symbol", "?"),
                            )
                            # Stamp: recovery attempted AND selected
                            _direct_quote_recovery_audit.update({
                                "attempted": True,
                                "selected":  True,
                                "contract":  str(opt.get("symbol") or ""),
                                "bid":       _direct_bid_pro,
                                "ask":       _direct_ask_pro,
                                "mid":       _direct_mid_pro,
                                "failure":   None,
                            })
                        else:
                            # Direct quote fetched but re-check still rejects.
                            # Stamp: recovery attempted, quality re-failure.
                            _direct_quote_recovery_audit.update({
                                "attempted":             True,
                                "selected":              False,
                                "contract":              str(opt.get("symbol") or ""),
                                "failure":               str(pro_reason),
                                "quality_recheck_failed": True,
                                "direct_bid_at_recheck": _direct_bid_pro,
                                "direct_ask_at_recheck": _direct_ask_pro,
                            })
                    elif _rv_pro.get("action") == "REJECT_DIRECT_ZERO":
                        # P1: name the specific failure — zero bid/ask on direct quote
                        pro_reason = "DIRECT_QUOTE_ZERO_BID_ASK"
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected":  False,
                            "failure":   "DIRECT_QUOTE_ZERO_BID_ASK",
                        })
                    elif _rv_pro.get("action") == "SKIP_BUDGET_EXHAUSTED":
                        # ``_direct_quote_recovery_audit`` is a REQUEST-level
                        # aggregate. Never overwrite ``attempted``,
                        # ``selected``, or the selected contract from a later
                        # budget-skipped candidate — earlier candidates in the
                        # same request may already have made a real provider
                        # call or produced a survivor. OR-only semantics:
                        # ``budget_skipped=True`` sticks once any later
                        # candidate is unattempted; per-candidate attribution
                        # lives in ``direct_quote_unattempted_symbols``.
                        _direct_quote_recovery_audit["budget_skipped"] = True
                        _direct_quote_recovery_audit["budget_skip_reason"] = (
                            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
                        )
                    elif _rv_pro.get("action") == "REJECT_UNAVAILABLE":
                        pro_reason = _rv_pro.get("reason_code") or "QUOTE_FETCH_FAILED"
                        _direct_quote_recovery_audit.update({
                            "attempted": True,
                            "selected":  False,
                            "failure":   str(_rv_pro.get("reason_code") or "QUOTE_FETCH_FAILED"),
                        })
                    # SKIP_NOT_MARKET_HOURS / SKIP_NOT_REVALIDATABLE: original reason
                    # stands and audit is left unstamped — the branch did not run,
                    # so attempted stays False by design.
                # ── end P0A/FIX-2 ────────────────────────────────────────────

                if pro_tier == "REJECT":
                    _record_recovery_candidate_outcome(
                        opt,
                        _pro_recovery_candidate_reason(
                            quality_reason=pro_reason,
                            recovery_action=_pro_recovery_action,
                            recovery_reason=_pro_recovery_reason,
                            fresh_quote_recovered=_pro_fresh_quote_recovered,
                        ),
                    )
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
            _candidate_result_reason = result
            _recovery_action = None

            # ── P0A: direct quote revalidation ────────────────────────────
            # When the chain row produced a revalidatable reject (zero/missing
            # bid-ask, bid_below_0.1, NO_CHAIN_DATA, zero liquidity) AND the
            # market is open and the request budget allows it, fetch a direct
            # option quote and re-run quality checks against the fresh quote.
            # Safety rules (spread, premium, affordability, capital) are
            # re-enforced against the direct quote — never bypassed.
            # One context enforces the hard cap across every DTE probe.
            if (
                result is not None
                and _should_revalidate(result)
                and not opt.get("_duplicate_quote_authority")
            ):
                _sym_for_count = str(opt.get("symbol") or "").strip().upper()
                if _sym_for_count and _sym_for_count not in request_context.direct_quote_eligible_symbols:
                    request_context.direct_quote_eligible_symbols.add(_sym_for_count)
                    request_context.direct_quote_eligible_candidates = len(
                        request_context.direct_quote_eligible_symbols
                    )
                _structural_skip = _structural_direct_quote_skip(
                    self,
                    opt,
                    direction=direction,
                    ticker=ticker,
                    underlying_price=float(underlying_price or 0.0),
                    today=today,
                    selector_budget=float(budget or 0.0),
                    request_context=request_context,
                )
                _rv = (
                    {
                        "action": "SKIP_STRUCTURAL",
                        "reason_code": _structural_skip.get("skip_reason"),
                        "direct_quote_used": False,
                        "opt_updated": None,
                        "audit": _structural_skip,
                    }
                    if _structural_skip
                    else _revalidate_direct(
                        self.data_broker,
                        opt,
                        result,
                        request_context=request_context,
                    )
                )
                _rv_action = _rv.get("action")
                _recovery_action = _rv_action
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
                        # Audit blocker 4 fix: _revalidate_direct already
                        # durably persisted DIRECT_QUOTE_RECOVERED_CHAIN_ZERO/
                        # transient=False to the selector recovery cursor at
                        # the transport-recovery stage, before this quality
                        # re-check ran. Now that the re-check has actually
                        # rejected the candidate, correct that record so the
                        # cursor never remains classified as a successful
                        # recovered candidate for a symbol that failed
                        # quality -- persist the real final reason instead.
                        # Any persistence failure here propagates naturally
                        # (SelectorRecoveryCursorPersistFailed /
                        # SelectorRecoveryOwnershipLost) to the same
                        # existing catch sites that already handle every
                        # other cursor write in this selector call.
                        _correct_recovered_cursor_disposition(
                            request_context,
                            str(opt.get("symbol") or ""),
                            final_reason=str(_result2),
                            provider_timestamp=(
                                _rv.get("audit", {}).get("provider_timestamp")
                            ),
                        )
                        # P0 PR #302 Fix 2: stamp quality re-failure audit so
                        # operators can distinguish "zero bid/ask" from "real
                        # bid/ask but spread/OI/volume gate re-fired".
                        try:
                            if not _direct_quote_recovery_audit.get("selected"):
                                _dq_bid_rf = float(_rv["audit"].get("direct_bid") or 0)
                                _dq_ask_rf = float(_rv["audit"].get("direct_ask") or 0)
                                _direct_quote_recovery_audit.update({
                                    "attempted":             True,
                                    "selected":              False,
                                    "failure":               str(_result2),
                                    "quality_recheck_failed": True,
                                    "direct_bid_at_recheck": _dq_bid_rf,
                                    "direct_ask_at_recheck": _dq_ask_rf,
                                })
                        except Exception:
                            pass
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
                elif _rv_action == "SKIP_BUDGET_EXHAUSTED":
                    _candidate_result_reason = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
                    try:
                        # Request-level aggregate: OR ``budget_skipped``
                        # in and record the reason. Do NOT overwrite
                        # ``attempted``, ``selected``, or the recorded
                        # contract — earlier candidates in this request
                        # may already have made a real provider call or
                        # produced a survivor. Per-candidate detail lives
                        # in ``direct_quote_unattempted_symbols``.
                        _direct_quote_recovery_audit["budget_skipped"] = True
                        _direct_quote_recovery_audit["budget_skip_reason"] = (
                            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
                        )
                    except Exception:
                        pass
                elif _rv_action == "REJECT_UNAVAILABLE":
                    # P1: quote source unavailable (network/auth failure)
                    result = _rv.get("reason_code") or "QUOTE_FETCH_FAILED"
                    _candidate_result_reason = result
                elif _rv_action == "SKIP_STRUCTURAL":
                    # Keep the aggregate reject bucket for diagnostics, but
                    # attribute the candidate's actual terminal disposition to
                    # the structural prefilter. Otherwise an unrelated far row
                    # is misreported as retryable chain-zero evidence.
                    _candidate_result_reason = _rv.get("reason_code") or result
                # SKIP_NOT_MARKET_HOURS / SKIP_NOT_REVALIDATABLE:
                # fall through with original chain reject reason unchanged.
            # ── end P0A ───────────────────────────────────────────────────

            if result is None:
                survivors.append(opt)
            else:
                _record_recovery_candidate_outcome(
                    opt,
                    (
                        _candidate_result_reason
                        if _recovery_action in {
                            "SKIP_STRUCTURAL",
                            "SKIP_BUDGET_EXHAUSTED",
                        }
                        else result
                    ),
                )
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
            # The recovery final-reason resolver applies deferred-recovery-only
            # precedence (budget exhaustion, transient dominance, full-set
            # affordability accounting). Running it against ORDINARY selector
            # failures would rewrite the truthful base-SHA _obs_reason and
            # pollute paper/live diagnostics and downstream retry classification.
            # Scope it strictly to deferred-breach contexts.
            if (
                request_context is not None
                and str(
                    getattr(request_context, "selector_request_kind", "")
                ).strip().upper()
                == SELECTOR_REQUEST_KIND_DEFERRED_BREACH
            ):
                try:
                    from ap.selector_retry_policy import (
                        resolve_selector_recovery_final_reason,
                    )
                    _cursor_attempted = dict(
                        (request_context.recovery_cursor or {}).get(
                            "attempted_symbols"
                        )
                        or {}
                    )
                    # The cursor is durable audit history, not authority for
                    # a refreshed candidate universe.  Keep every historical
                    # record in the cursor, but expose only current-universe
                    # records to the final-reason reducer so a disappeared
                    # symbol cannot revive an old retryable outcome.
                    _current_cursor_attempted = {
                        _symbol: _record
                        for _symbol, _record in _cursor_attempted.items()
                        if _symbol in _recovery_candidate_universe
                    }
                    _structural_reasons = {
                        item.get("symbol"): item.get("skip_reason")
                        for item in request_context.structural_skips
                        if isinstance(item, dict) and item.get("symbol")
                    }
                    _unattempted_symbols = set(
                        getattr(
                            request_context,
                            "direct_quote_unattempted_set",
                            set(),
                        )
                        or set()
                    )
                    _accounted_symbols = (
                        set(_recovery_candidate_outcomes)
                        | set(_structural_reasons)
                        | _unattempted_symbols
                    )
                    _candidate_accounting_complete = bool(
                        _recovery_candidate_universe
                        and not _recovery_candidate_universe_has_missing_identity
                        and _recovery_candidate_universe.issubset(_accounted_symbols)
                        and not _unattempted_symbols
                    )
                    request_context.selector_candidate_universe_count = len(
                        _recovery_candidate_universe
                    )
                    request_context.selector_candidate_accounted_count = len(
                        _accounted_symbols & _recovery_candidate_universe
                    )
                    request_context.selector_candidate_accounting_complete = (
                        _candidate_accounting_complete
                    )
                    request_context.selector_candidate_outcomes = dict(
                        list(_recovery_candidate_outcomes.items())[-200:]
                    )
                    _ctx_refresh_diagnostics(request_context)
                    _final_reason = resolve_selector_recovery_final_reason({
                        "budget_exhausted_stage": request_context.budget_exhausted_stage,
                        "budget_exhausted_detail": request_context.budget_exhausted_detail,
                        "actual_limit_reached": bool(
                            request_context.budget_exhausted_stage
                            and request_context.budget_exhausted_detail
                        ),
                        "eligible_unattempted_symbols": list(
                            request_context.direct_quote_unattempted_symbols
                        ),
                        "attempted_results": _current_cursor_attempted,
                        "structural_skip_results": _structural_reasons,
                        "candidate_outcomes": dict(_recovery_candidate_outcomes),
                        "current_candidate_universe": list(
                            _recovery_candidate_universe
                        ),
                        "candidate_accounting_complete": _candidate_accounting_complete,
                        "candidate_universe_count": len(_recovery_candidate_universe),
                        "candidate_accounted_count": len(
                            _accounted_symbols & _recovery_candidate_universe
                        ),
                        "quality_rejections": {
                            _normalize_reason_code(key): value
                            for key, value in _rejections.items()
                        },
                        "market_truth_outcome": (
                            (request_context.recovery_cursor or {}).get(
                                "last_market_truth_outcome"
                            )
                        ),
                        "market_truth_reason": None,
                    })
                except Exception:
                    pass
            _attach_selector_failure(
                plan,
                reason_code=_final_reason,
                canonical_selector_reason=_final_reason,
                last_observed_selector_reason=_obs_reason,
                operational_reason=_selector_operational_reason(request_context),
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
                selection_diagnostics=_selector_request_diagnostics(request_context),
            )
            return None

        if _PRO_QUALITY_ENABLED:
            log.info("[%s] %d contracts passed pro quality | A=%d B=%d",
                     ticker, len(survivors), _pro_tiers.get("A", 0), _pro_tiers.get("B", 0))
        else:
            log.info("[%s] %d contracts passed quality filter", ticker, len(survivors))

        # ── C. RANK ───────────────────────────────────────────────────────────
        _playbook_candidate_ctx = None
        if (
            request_context is not None
            and bool(getattr(request_context, "playbook_enabled", False))
            and request_context.playbook_spec is not None
            and expiration_override is not None
        ):
            try:
                _playbook_candidate_ctx = build_playbook_candidate_context(
                    request_context.playbook_spec,
                    survivors,
                )
                request_context.playbook_candidate_context = dict(_playbook_candidate_ctx)
                if isinstance(request_context.playbook_audit, dict):
                    request_context.playbook_audit["atm_strike"] = _playbook_candidate_ctx.get("atm_strike")
                    request_context.playbook_audit["one_step_otm_strike"] = _playbook_candidate_ctx.get("one_step_otm_strike")
                    request_context.playbook_audit["preferred_strikes"] = list(_playbook_candidate_ctx.get("preferred_strikes") or [])
                    request_context.playbook_audit["strike_band_low"] = _playbook_candidate_ctx.get("strike_band_low")
                    request_context.playbook_audit["strike_band_high"] = _playbook_candidate_ctx.get("strike_band_high")
            except Exception:
                _playbook_candidate_ctx = None
        elif (
            _preferred_strike_audit.get("enabled")
            and _preferred_strike_audit.get("primary_strike") is not None
        ):
            # PR #396 amendment: on the direct (non-ladder) path, if the
            # playbook flag is on and the shared trigger-anchored authority
            # produced a preferred strike, build a compatible candidate context
            # from the surviving contracts through that same authority. This
            # avoids freezing tiers from the unfiltered chain: if the original
            # primary and adjacent contracts fail hard gates, the nearest valid
            # survivor becomes the recomputed primary just as it does on the
            # ladder path.
            try:
                _survivor_preference = resolve_trigger_anchored_preferred_strikes(
                    side=direction,
                    trigger_price=_safe_plan_attr(plan, "trigger_price", None),
                    underlying_fallback=underlying_price,
                    candidate_strikes=[opt.get("strike") for opt in survivors],
                )
                _primary_strike = _survivor_preference.primary_strike
                if _primary_strike is None:
                    raise ValueError("no valid survivor strike preference")
                _primary_strike = float(_primary_strike)
                _adjacent_strike = _survivor_preference.adjacent_otm_strike
                _adjacent_strike = (
                    float(_adjacent_strike) if _adjacent_strike is not None else None
                )
                _preferred_list = [_primary_strike]
                if _adjacent_strike is not None:
                    _preferred_list.append(_adjacent_strike)
                _per_symbol: dict[str, dict] = {}
                for _s_opt in survivors:
                    _sym = str(_s_opt.get("symbol") or "")
                    _s_strike = _safe_float(_s_opt.get("strike"))
                    if _s_strike is None:
                        _per_symbol[_sym] = {
                            "strike_policy_tier": STRIKE_POLICY_TIER_NONMATCH,
                            "strike_policy_label": STRIKE_POLICY_LABEL_NONMATCH,
                            "strike_policy_match": False,
                            "distance_to_preferred": None,
                        }
                        continue
                    if abs(_s_strike - _primary_strike) < 1e-9:
                        _tier, _label = (
                            STRIKE_POLICY_TIER_ATM, STRIKE_POLICY_LABEL_ATM,
                        )
                    elif (
                        _adjacent_strike is not None
                        and abs(_s_strike - _adjacent_strike) < 1e-9
                    ):
                        _tier, _label = (
                            STRIKE_POLICY_TIER_ONE_STEP_OTM,
                            STRIKE_POLICY_LABEL_ONE_STEP_OTM,
                        )
                    else:
                        _tier, _label = (
                            STRIKE_POLICY_TIER_NONMATCH,
                            STRIKE_POLICY_LABEL_NONMATCH,
                        )
                    _per_symbol[_sym] = {
                        "strike_policy_tier": _tier,
                        "strike_policy_label": _label,
                        "strike_policy_match": _tier != STRIKE_POLICY_TIER_NONMATCH,
                        "distance_to_preferred": (
                            0.0
                            if _tier != STRIKE_POLICY_TIER_NONMATCH
                            else min(abs(_s_strike - _p) for _p in _preferred_list)
                        ),
                    }
                _playbook_candidate_ctx = {
                    "atm_strike": _primary_strike,
                    "one_step_otm_strike": _adjacent_strike,
                    "preferred_strikes": _preferred_list,
                    "strike_band_low": min(_preferred_list),
                    "strike_band_high": max(_preferred_list),
                    "per_symbol": _per_symbol,
                    "anchor_source": _survivor_preference.anchor_source,
                    "anchor_price": _survivor_preference.anchor_price,
                }
                if request_context is not None:
                    request_context.playbook_candidate_context = dict(_playbook_candidate_ctx)
            except Exception:
                _playbook_candidate_ctx = None
        scored = []
        for opt in survivors:
            s = self._rank_score(
                opt, budget,
                expected_move_pct=_expected_move_pct,
                underlying_price=underlying_price or 0.0,
                tier=_safe_plan_attr(plan, "tier", "B") or "B",
            )
            if _playbook_candidate_ctx is not None:
                _symbol = str(opt.get("symbol") or "")
                _playbook_fit = ((_playbook_candidate_ctx.get("per_symbol") or {}).get(_symbol) or {})
                opt["_playbook_strike_policy_tier"] = int(
                    _playbook_fit.get("strike_policy_tier", STRIKE_POLICY_TIER_NONMATCH)
                )
                opt["_playbook_strike_policy_label"] = str(
                    _playbook_fit.get("strike_policy_label", STRIKE_POLICY_LABEL_NONMATCH)
                )
                opt["_playbook_strike_match"] = bool(_playbook_fit.get("strike_policy_match"))
                opt["_playbook_distance_to_preferred"] = _playbook_fit.get("distance_to_preferred")
                opt["_playbook_original_rank_score"] = s
            scored.append((s, opt))

        if _playbook_candidate_ctx is not None:
            def _playbook_sort_key(item):
                _score, _opt = item
                return (
                    int(_opt.get("_playbook_strike_policy_tier", STRIKE_POLICY_TIER_NONMATCH)),
                    -float(_score),
                )

            scored.sort(key=_playbook_sort_key)
        else:
            scored.sort(key=lambda x: x[0], reverse=True)

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

        _MIN_ACCEPTABLE_PREMIUM = float(os.getenv("MIN_ACCEPTABLE_PREMIUM_PER_CONTRACT", "50"))
        _MAX_UPGRADE_PREMIUM    = float(os.getenv("MAX_IDEAL_PREMIUM_PER_CONTRACT", "350"))
        _MIN_DELTA              = float(os.getenv("MIN_CONTRACT_DELTA", "0.10"))
        _MAX_OTM_PCT            = float(os.getenv("MAX_OTM_PCT", "0.12"))
        _final_prem_cap         = _get_max_premium(ticker)
        _effective_budget_used, _max_trade_cap, _budget_was_clipped = _effective_budget(budget)
        _candidate_table_top_n = int(os.getenv("SELECTOR_CANDIDATE_TABLE_TOP_N", "15"))
        _final_candidate_rejections: list[dict] = []
        # Amendment 2: deferred cheap-contract fallback.
        # When ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE=true a cheap candidate that
        # otherwise passes all gates is retained here rather than selected
        # immediately.  We continue evaluating every later ranked candidate and
        # only use _cheap_fallback when no normal-premium candidate passes.
        _cheap_fallback: Optional[SelectedContract] = None

        def _candidate_audit_payload(selected_symbol, selected_reason):
            try:
                payload = _build_candidate_audit(
                    scored,
                    underlying_price or 0.0,
                    selected_symbol=selected_symbol,
                    selected_reason=selected_reason,
                    rejections=_rejections,
                    top_n=_candidate_table_top_n,
                )
                if isinstance(payload, dict):
                    payload["final_candidate_rejections"] = list(_final_candidate_rejections)
                return payload
            except Exception:
                return None

        def _record_final_rejection(
            rank_idx: int,
            opt: dict,
            score: float,
            selected_candidate: Optional[SelectedContract],
            *,
            stage: str,
            reason_code: str,
            explanation: str,
            inputs: Optional[dict] = None,
            thresholds: Optional[dict] = None,
            context: Optional[dict] = None,
            tradeability_diag: Optional[dict] = None,
        ) -> None:
            # Audit blocker 4 fix (continued): any candidate reaching this
            # rejection choke point that was earlier patched via direct-
            # quote transport recovery (opt["_direct_quote_used"] is True,
            # set at the patch site in contract_quote_revalidator.py) may
            # already have a premature DIRECT_QUOTE_RECOVERED_CHAIN_ZERO/
            # transient=False record in the durable cursor from that
            # transport-recovery stage -- correct it here with the real
            # final rejection reason, covering every later-stage check
            # (affordability, delta, moneyness, premium) in one place
            # rather than duplicating the correction at each call site.
            if opt.get("_direct_quote_used"):
                _correct_recovered_cursor_disposition(
                    request_context,
                    str(opt.get("symbol") or ""),
                    final_reason=str(reason_code or "UNKNOWN_FAIL_CLOSED"),
                    provider_timestamp=opt.get("_direct_quote_fetched_at"),
                )
            candidate_row = _build_candidate_row(
                opt,
                rank_score=score,
                rejected_at_step=stage,
                rejection_reason=reason_code,
                selected=False,
            )
            candidate_row["rank"] = rank_idx + 1
            candidate_row["contract"] = candidate_row["symbol"]
            candidate_row["reason_code"] = reason_code
            candidate_row["explanation"] = explanation
            if selected_candidate is not None:
                candidate_row["premium_per_contract_usd"] = _safe_float(selected_candidate.premium_per_contract)
                candidate_row["affordable_contracts"] = int(selected_candidate.affordable_contracts or 0)
                candidate_row["pricing_basis"] = selected_candidate.pricing_basis
            if tradeability_diag:
                candidate_row["tradeability_diag"] = dict(tradeability_diag)
            _final_candidate_rejections.append(candidate_row)
            self._emit_selector_event(
                plan,
                stage=stage,
                decision="REJECT",
                reason_code=reason_code,
                explanation=explanation,
                contract=opt.get("symbol"),
                inputs=inputs,
                thresholds=thresholds,
                context=context,
            )
            failure = {
                "stage": stage,
                "reason_code": reason_code,
                "explanation": explanation,
            }
            if tradeability_diag:
                failure["tradeability_diag"] = dict(tradeability_diag)
            self._set_last_failure(failure)

        selected = None
        for _rank_idx, (candidate_score, candidate_opt) in enumerate(scored):
            candidate = self._build_selected(candidate_opt, candidate_score, budget, today)
            if candidate is None:
                _record_final_rejection(
                    _rank_idx,
                    candidate_opt,
                    candidate_score,
                    None,
                    stage="contract_build",
                    reason_code="CONTRACT_BUILD_FAILED",
                    explanation=f"_build_selected() returned None for {candidate_opt.get('symbol', '?')} — internal build error",
                )
                continue

            _candidate_context = {
                "candidate_rank": _rank_idx + 1,
                "candidate_table": _candidate_audit_payload(candidate.contract_symbol, candidate.selection_reason),
            }

            if _budget_was_clipped and _rank_idx == 0:
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
                    contract=candidate.contract_symbol,
                    inputs={
                        "upstream_budget": _safe_float(budget),
                        "effective_budget": _safe_float(_effective_budget_used),
                        "max_trade_usd": _safe_float(_max_trade_cap),
                        "premium_per_contract": _safe_float(candidate.premium_per_contract),
                    },
                    thresholds={"max_trade_usd": _max_trade_cap},
                    context={"budget_clipped": True},
                )

            # Amendment 2: _defer_as_cheap is reset every iteration.
            # Set to True when ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE=true and the
            # candidate is cheap so the bottom of the loop defers instead of selects.
            _defer_as_cheap = False
            if candidate.premium_per_contract < _MIN_ACCEPTABLE_PREMIUM:
                _allow_cheap = os.getenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "false").lower() == "true"
                if not _allow_cheap:
                    _record_final_rejection(
                        _rank_idx,
                        candidate_opt,
                        candidate_score,
                        candidate,
                        stage="cheap_contract_gate",
                        reason_code="CHEAP_CONTRACT_NO_UPGRADE",
                        explanation=(
                            f"{candidate.contract_symbol} premium ${candidate.premium_per_contract:.0f} below "
                            f"${_MIN_ACCEPTABLE_PREMIUM:.0f}; evaluating next ranked candidate."
                        ),
                        inputs={
                            "selected_contract": candidate.contract_symbol,
                            "selected_premium": round(candidate.premium_per_contract, 2),
                        },
                        thresholds={
                            "min_acceptable_premium": _MIN_ACCEPTABLE_PREMIUM,
                            "max_upgrade_premium": _MAX_UPGRADE_PREMIUM,
                            "allow_cheap_if_only_choice": _allow_cheap,
                        },
                        context=_candidate_context,
                    )
                    continue
                # Flag is true: fall through all remaining gates — if this candidate
                # passes everything, defer it as a fallback rather than selecting it.
                # We keep evaluating later candidates for a normal-premium winner.
                _defer_as_cheap = True

            if candidate.affordable_contracts < 1:
                if _selector_mode != "LIVE":
                    if candidate.premium_per_contract > self.max_premium:
                        _record_final_rejection(
                            _rank_idx,
                            candidate_opt,
                            candidate_score,
                            candidate,
                            stage="affordability_gate",
                            reason_code="PAPER_PREMIUM_CAP",
                            explanation=(
                                f"Paper premium cap: ${candidate.premium_per_contract:.0f} "
                                f"> max ${self.max_premium:.0f} — valid contract but too expensive for paper."
                            ),
                            inputs={
                                "premium_per_contract": _safe_float(candidate.premium_per_contract),
                                "max_premium": _safe_float(self.max_premium),
                            },
                            thresholds={"max_premium": self.max_premium},
                            context=dict({"mode": self.mode}, **_candidate_context),
                        )
                        continue
                    log.warning("[%s] budget $%.0f < premium $%.0f -- forcing 1 contract",
                                ticker, budget, candidate.premium_per_contract)
                    self._emit_selector_event(
                        plan,
                        stage="affordability_gate",
                        decision="ALLOW",
                        reason_code="FORCED_1_FOR_PAPER",
                        explanation=(
                            f"Paper override: budget ${budget:.0f} < premium "
                            f"${candidate.premium_per_contract:.0f} — forcing 1 contract. "
                            "NOT valid for live trading."
                        ),
                        contract=candidate.contract_symbol,
                        inputs={
                            "budget": _safe_float(budget),
                            "premium_per_contract": _safe_float(candidate.premium_per_contract),
                        },
                        context={"mode": self.mode, "simulation_override": True},
                    )
                    candidate = SelectedContract(
                        contract_symbol=candidate.contract_symbol,
                        expiration=candidate.expiration,
                        strike=candidate.strike,
                        option_type=candidate.option_type,
                        bid=candidate.bid,
                        ask=candidate.ask,
                        mid=candidate.mid,
                        spread_pct=candidate.spread_pct,
                        delta=candidate.delta,
                        open_interest=candidate.open_interest,
                        volume=candidate.volume,
                        premium_per_share=candidate.premium_per_share,
                        premium_per_contract=candidate.premium_per_contract,
                        affordable_contracts=1,
                        scoring_price_per_share=candidate.scoring_price_per_share,
                        execution_price_per_share=candidate.execution_price_per_share,
                        effective_budget=candidate.effective_budget,
                        budget_clipped=candidate.budget_clipped,
                        pricing_basis=candidate.pricing_basis,
                        selection_reason=candidate.selection_reason + " [forced_1]",
                        selection_score=candidate.selection_score,
                        dte=candidate.dte,
                        candidate_audit=candidate.candidate_audit,
                    )
                else:
                    _equity = _safe_float(_sizing_val(plan, "account_equity", "equity", default=0)) or None
                    _max_pos_pct = _safe_float(_sizing_val(plan, "risk_pct", "max_position_pct", default=0)) or None
                    _max_afford_prem = _safe_float(_sizing_val(plan, "max_affordable_premium", default=0)) or None
                    _ctx_budget = _safe_float(budget)
                    _underlying = _safe_float(underlying_price) or None
                    _tradeability_diag = {
                        "equity": _equity,
                        "max_position_pct": _max_pos_pct,
                        "max_trade_usd": _safe_float(_max_trade_usd()),
                        "max_affordable_premium": _max_afford_prem,
                        "underlying_price": _underlying,
                        "budget": _ctx_budget,
                        "budget_source": _budget_constraints.get("selected_source"),
                        "budget_constraints": _budget_constraints,
                        "selected_dte": _safe_float(getattr(candidate, "dte", None)),
                        "near_atm_premium_estimate": _safe_float(candidate.premium_per_contract),
                        "cheapest_quality_survivor_premium": _safe_float(candidate.premium_per_contract),
                        "classification": "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                        "premium_per_share": _safe_float(candidate.premium_per_share),
                        "premium_per_contract_usd": _safe_float(candidate.premium_per_contract),
                        "ask_cost": round(_safe_float(candidate.premium_per_contract), 2),
                        "qty_attempted": 1,
                        "projected_reserved_cost": round(_safe_float(candidate.premium_per_contract), 2),
                        "projected_reserved_cost_usd": round(_safe_float(candidate.premium_per_contract), 2),
                    }
                    _diag_summary = (
                        f"equity=${_equity or 0:.0f} budget=${_ctx_budget or 0:.0f} "
                        f"premium=${candidate.premium_per_contract:.0f} "
                        f"underlying=${_underlying or 0:.2f} dte={getattr(candidate,'dte',None)}"
                    )
                    _explanation = (
                        f"Quality contract {candidate.contract_symbol} @ "
                        f"${candidate.premium_per_contract:.0f}/contract exceeds budget "
                        f"${_ctx_budget or 0:.0f} — structurally untradeable for this account "
                        f"size [{_diag_summary}]. No ticker blacklist; eligible again if a "
                        f"cheaper quality contract appears or the account grows."
                    )
                    self._set_last_failure({
                        "stage": "affordability_gate",
                        "reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                        "explanation": _explanation,
                        "tradeability_diag": _tradeability_diag,
                    })
                    # return None after ranked fallback if no later candidate passes.
                    _record_final_rejection(
                        _rank_idx,
                        candidate_opt,
                        candidate_score,
                        candidate,
                        stage="affordability_gate",
                        reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE",  # return None if no later candidate passes.
                        explanation=_explanation,
                        inputs=_tradeability_diag,
                        thresholds={"min_contracts": 1},
                        context=_candidate_context,
                        tradeability_diag=_tradeability_diag,
                    )
                    continue

            if candidate.delta is not None and candidate.delta < _MIN_DELTA:
                _record_final_rejection(
                    _rank_idx,
                    candidate_opt,
                    candidate_score,
                    candidate,
                    stage="deep_otm_gate",
                    reason_code="DELTA_OUT_OF_RANGE",
                    explanation=f"Deep OTM gate: delta={candidate.delta:.2f} < min={_MIN_DELTA:.2f}",
                    inputs={"delta": candidate.delta, "strike": candidate.strike},
                    thresholds={"min_delta": _MIN_DELTA},
                    context=_candidate_context,
                )
                continue

            if underlying_price and underlying_price > 0 and candidate.strike > 0:
                _otm_pct = abs(candidate.strike - underlying_price) / underlying_price
                if _otm_pct > _MAX_OTM_PCT:
                    _record_final_rejection(
                        _rank_idx,
                        candidate_opt,
                        candidate_score,
                        candidate,
                        stage="moneyness_gate",
                        reason_code="MONEYNESS_OUT_OF_RANGE",
                        explanation=(
                            f"Moneyness gate: strike={candidate.strike:.2f} "
                            f"underlying={underlying_price:.2f} "
                            f"OTM={_otm_pct*100:.1f}% > max={_MAX_OTM_PCT*100:.0f}%"
                        ),
                        inputs={
                            "strike": _safe_float(candidate.strike),
                            "underlying_price": _safe_float(underlying_price),
                            "otm_pct": round(_otm_pct, 4),
                            "delta": _safe_float(candidate.delta),
                        },
                        thresholds={"max_otm_pct": _MAX_OTM_PCT},
                        context=_candidate_context,
                    )
                    continue

            if candidate.premium_per_contract > _final_prem_cap:
                _record_final_rejection(
                    _rank_idx,
                    candidate_opt,
                    candidate_score,
                    candidate,
                    stage="premium_gate",
                    reason_code="PREMIUM_CAP_EXCEEDED",
                    explanation=(
                        f"Final premium gate: ${candidate.premium_per_contract:.0f} "
                        f"> ${_final_prem_cap:.0f} per-ticker cap for {ticker}"
                    ),
                    inputs={
                        "premium_per_contract": _safe_float(candidate.premium_per_contract),
                        "ticker_cap": _safe_float(_final_prem_cap),
                    },
                    thresholds={"ticker_premium_cap": _final_prem_cap},
                    context=_candidate_context,
                )
                continue

            # Amendment 2: candidate passed all gates.
            # If it is cheap and the flag is set, store as deferred fallback and
            # keep evaluating — a normal-premium candidate later in the ranking wins.
            if _defer_as_cheap:
                if _cheap_fallback is None:
                    _cheap_fallback = candidate
                    log.debug(
                        "[%s] cheap_contract_fallback stored rank=%d %s @ $%.0f — "
                        "continuing evaluation for normal-premium candidate",
                        ticker, _rank_idx + 1,
                        candidate.contract_symbol, candidate.premium_per_contract,
                    )
                continue

            selected = candidate
            if candidate_opt.get("_duplicate_quote_authority"):
                _direct_quote_recovery_audit.update({
                    "attempted": True,
                    "selected": True,
                    "contract": str(candidate.contract_symbol or ""),
                    "bid": _safe_float(candidate.bid, 0.0),
                    "ask": _safe_float(candidate.ask, 0.0),
                    "mid": _safe_float(candidate.mid, 0.0),
                    "failure": None,
                    "duplicate_quote_authority": True,
                })
            # Audit Blocker 2, true two-stage fix: this is the one place in
            # the whole method where a candidate has passed EVERY gate
            # (quality_filter's spread/OI/volume, then affordability,
            # delta, premium, cheap-contract-gate) -- the only point at
            # which "this direct-quote-recovered candidate genuinely
            # succeeded" is actually, finally true. The premature write
            # that used to happen inside revalidate_with_direct_quote
            # immediately after a valid transport-level quote (before any
            # of these checks ran) has been removed entirely -- there is
            # no longer an earlier "recovered" record for a crash to catch
            # in a false state. A rejection anywhere along the way is
            # persisted at its own rejection point (via
            # _correct_recovered_cursor_disposition, called from the
            # quality-refail branch above and from
            # _record_final_rejection); this call fires only for the
            # single candidate that actually wins.
            if candidate_opt.get("_direct_quote_used"):
                _ctx_persist_attempt(
                    request_context,
                    str(candidate_opt.get("symbol") or ""),
                    result_reason="DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
                    transient=False,
                    provider_timestamp=candidate_opt.get("_direct_quote_fetched_at"),
                )
            break

        # Amendment 1: a valid candidate passed all final gates.
        # Higher-ranked rejections are preserved in _final_candidate_rejections
        # (embedded in candidate_audit["final_candidate_rejections"]) but must
        # NOT be the authoritative last_failure for this invocation.
        if selected is not None:
            self._set_last_failure(None)
            _clear_selector_failure(plan)  # belt-and-suspenders: remove any stale rejection

        # Amendment 2: no normal-premium candidate found — activate cheap fallback.
        if selected is None and _cheap_fallback is not None:
            selected = _dc_replace(
                _cheap_fallback,
                selection_reason="cheap_contract_only_choice",
            )
            log.info(
                "[%s] cheap_contract_only_choice: %s @ $%.0f — "
                "%d later candidate(s) evaluated and rejected",
                ticker, selected.contract_symbol,
                selected.premium_per_contract,
                len(_final_candidate_rejections),
            )
            self._set_last_failure(None)
            _clear_selector_failure(plan)

        if selected is None:
            # Amendment 3: authoritative and deterministic final failure reason.
            #
            # Policy: choose the reason_code with the highest occurrence count
            # across all final-gate rejections; ties broken by _FINAL_REASON_PRECEDENCE
            # (most structurally severe first).  This prevents the last evaluated
            # candidate from accidentally determining the operator-visible failure.
            #
            # _FINAL_REASON_PRECEDENCE (fixed, documented):
            #   CAPITAL_NO_REMAINING        — no capital at all
            #   INVALID_POSITION_BUDGET     — budget value is corrupt / negative
            #   UNTRADEABLE_FOR_ACCOUNT_SIZE — every contract exceeds account size
            #   DELTA_OUT_OF_RANGE          — chain has no quality-delta contracts (delta < min)
            #   MONEYNESS_OUT_OF_RANGE      — chain strikes are too far from price (OTM > max)
            #   CHEAP_CONTRACT_NO_UPGRADE   — only sub-minimum-premium contracts exist
            #   PREMIUM_CAP_EXCEEDED        — per-ticker cap blocks all candidates
            #   PAPER_PREMIUM_CAP           — paper-mode cap blocks all candidates
            #   CONTRACT_BUILD_FAILED       — internal _build_selected() error
            #   NO_AFFORDABLE_CONTRACT      — generic fallback
            _FINAL_REASON_PRECEDENCE = _SELECTOR_FINAL_REASON_PRECEDENCE
            _reason_counts: dict[str, int] = {}
            for _rej in _final_candidate_rejections:
                _rc = str(_rej.get("reason_code") or "UNKNOWN")
                _reason_counts[_rc] = _reason_counts.get(_rc, 0) + 1

            if _reason_counts:
                _max_count = max(_reason_counts.values())
                _tied = [r for r, c in _reason_counts.items() if c == _max_count]
                _final_reason_code = next(
                    (p for p in _FINAL_REASON_PRECEDENCE if p in _tied),
                    _tied[0],
                )
            else:
                _final_reason_code = "NO_AFFORDABLE_CONTRACT"

            _final_failure = next(
                (r for r in _final_candidate_rejections
                 if r.get("reason_code") == _final_reason_code),
                _final_candidate_rejections[0] if _final_candidate_rejections else None,
            )
            _final_explanation = (_final_failure or {}).get(
                "explanation",
                f"No ranked candidates survived final gates | chain={_sel_chain_rows} survivors={_sel_survivors}",
            )
            # Blocking defect fix: _record_final_rejection() left _last_failure pointing
            # at the last candidate's rejection.  Stamp the deterministic authoritative
            # reason so get_last_failure() always reflects the policy decision, not
            # whichever candidate happened to be evaluated last.
            # tradeability_diag is forwarded from the winning failure row when present
            # (UNTRADEABLE_FOR_ACCOUNT_SIZE carries per-contract cost evidence that
            # callers rely on for diagnostics).
            _authoritative_lf: dict = {
                "stage":       (_final_failure or {}).get("stage", "final_gate"),
                "reason_code": _final_reason_code,
                "explanation": _final_explanation,
            }
            if _final_failure and "tradeability_diag" in _final_failure:
                _authoritative_lf["tradeability_diag"] = _final_failure["tradeability_diag"]
            self._set_last_failure(_authoritative_lf)
            _failure_diagnostics = dict(_selector_request_diagnostics(request_context))
            _failure_diagnostics["final_candidate_rejections"] = list(_final_candidate_rejections)
            # Amendment 3: per-reason counts so operators can see the distribution
            # of failure modes without scanning every row in final_candidate_rejections.
            _failure_diagnostics["final_rejection_counts"] = dict(_reason_counts)
            _failure_diagnostics["pricing_basis"] = _pricing_basis_for_mode(_selector_mode)[1]
            _failure_diagnostics["sizing_context"] = dict(_sizing_ctx) if isinstance(_sizing_ctx, dict) else {}
            _failure_candidate_audit = _candidate_audit_payload(None, _final_reason_code)
            if _failure_candidate_audit is not None:
                _failure_diagnostics["candidate_table"] = _failure_candidate_audit
            _attach_selector_failure(
                plan,
                reason_code=_final_reason_code,
                canonical_selector_reason=_final_reason_code,
                last_observed_selector_reason=_final_reason_code,
                operational_reason=_selector_operational_reason(request_context),
                explanation=_final_explanation,
                chain_rows=_sel_chain_rows,
                survivor_count=_sel_survivors,
                reject_buckets=_sel_rejections,
                base_url=_sel_base_url,
                execution_mode=_sel_mode,
                best_rejected_candidate=_final_failure or _best_rejected_candidate,
                chain_quote_validity=_chain_quote_validity or None,
                direct_quote_recovery_audit=_direct_quote_recovery_audit,
                selection_diagnostics=_failure_diagnostics,
            )
            return None

        try:
            selected.candidate_audit = _candidate_audit_payload(
                selected.contract_symbol,
                selected.selection_reason,
            )
        except Exception:
            selected.candidate_audit = None
        _candidate_context = {"candidate_table": selected.candidate_audit} if selected.candidate_audit else None

        # ── F. UPDATE PLAN IN-PLACE ───────────────────────────────────────────
        # Default behavior remains mutation because downstream execution expects
        # the selected contract fields on the plan. Set mutate_plan=False only
        # for audit/replay callers that explicitly consume SelectedContract.
        _selection_diagnostics = _selector_request_diagnostics(request_context)
        # Attach the request-level direct-quote recovery audit so the caller
        # can verify aggregate truthfulness (attempted / selected /
        # budget_skipped) without needing a failure path to emit it.
        _selection_diagnostics["direct_quote_recovery_audit"] = dict(_direct_quote_recovery_audit)
        try:
            plan.setdefault("metadata", {})["selector_diagnostics"] = _selection_diagnostics
        except Exception:
            pass
        if isinstance(selected.candidate_audit, dict):
            selected.candidate_audit["selection_diagnostics"] = _selection_diagnostics
        if request_context is not None and isinstance(request_context.playbook_audit, dict):
            try:
                selected_rank, selected_opt = next(
                    (idx + 1, opt)
                    for idx, (_, opt) in enumerate(scored)
                    if str(opt.get("symbol") or "") == str(selected.contract_symbol or "")
                )
                request_context.playbook_audit["selected_initial_rank"] = selected_rank
                request_context.playbook_audit["selected_strike_policy_tier"] = int(
                    selected_opt.get("_playbook_strike_policy_tier", STRIKE_POLICY_TIER_NONMATCH)
                )
                request_context.playbook_audit["selected_strike_policy_label"] = str(
                    selected_opt.get("_playbook_strike_policy_label", STRIKE_POLICY_LABEL_NONMATCH)
                )
                request_context.playbook_audit["selected_original_rank_score"] = _safe_float(
                    selected_opt.get("_playbook_original_rank_score")
                )
            except Exception:
                pass
            self._set_playbook_audit(plan, request_context, request_context.playbook_audit)
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
                    "budget_source": _budget_constraints.get("selected_source"),
                    "budget_conflict": _budget_conflict,
                    "budget_constraints": _budget_constraints,
                    "sizing_context": dict(_sizing_ctx) if isinstance(_sizing_ctx, dict) else {},
                    "selection_diagnostics": _selection_diagnostics,
                    "premium_per_share": selected.premium_per_share,
                    "premium_per_contract_usd": selected.premium_per_contract,
                    "projected_reserved_cost_usd": selected.premium_per_contract * selected.affordable_contracts,
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
                        "budget_source": _budget_constraints.get("selected_source"),
                        "budget_conflict": _budget_conflict,
                        "budget_constraints": _budget_constraints,
                        "sizing_context": dict(_sizing_ctx) if isinstance(_sizing_ctx, dict) else {},
                        "selection_diagnostics": _selection_diagnostics,
                        "premium_per_share": selected.premium_per_share,
                        "premium_per_contract_usd": selected.premium_per_contract,
                        "projected_reserved_cost_usd": selected.premium_per_contract * selected.affordable_contracts,
                    })
                    plan.selector_metadata = meta
                except Exception:
                    pass
            # P1 fix: the post-mutation identity read MUST use the identical
            # ``execution_mode`` -> ``mode`` fallback chain used at capture
            # (see _original_execution_mode above). Comparing a fallback-resolved
            # capture against a non-fallback read made every plan that carries
            # legacy ``mode`` (but no ``execution_mode`` attribute) trip this
            # assertion as a false positive — 74 queue ERRORs/week in production
            # ("contract_selector_error: Selector mutation changed execution
            # identity") killing viable signals at contract selection.
            _post_execution_mode = (
                _safe_plan_attr(plan, "execution_mode", None)
                or _safe_plan_attr(plan, "mode", None)
            )
            if (
                _safe_plan_attr(plan, "client_id", None) != _original_client_id
                or _post_execution_mode != _original_execution_mode
                or _safe_plan_attr(plan, "signal_id", None) != _original_signal_id
            ):
                raise AssertionError("Selector mutation changed execution identity")
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
                "selection_diagnostics": _selection_diagnostics,
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

    def _fetch_chain_with_price(
        self,
        ticker: str,
        direction: str,
        *,
        expiration_override: Optional[str] = None,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> tuple[list[dict], Optional[float]]:
        option_type = direction.lower()
        return self._fetch_tradier_chain(
            ticker,
            option_type,
            expiration_override=expiration_override,
            request_context=request_context,
        )

    def _fetch_chain(self, ticker: str, direction: str) -> list[dict]:
        chain, _ = self._fetch_chain_with_price(ticker, direction)
        return chain

    def _throttle_before_market_data_call(
        self,
        endpoint: str,
        symbol: str,
        *,
        context: str,
        request_context: Optional[SelectorRequestContext] = None,
    ):
        try:
            from ap.tradier_market_data_throttle import before_market_data_call
        except Exception as exc:
            log.error(
                "[%s] selector throttle import failed endpoint=%s context=%s err=%s",
                symbol, endpoint, context, exc,
            )
            _ctx_note_throttle_issue(
                request_context,
                endpoint=endpoint,
                symbol=symbol,
                context=context,
                phase="import",
                error=exc,
            )
            return None
        try:
            token = before_market_data_call(endpoint, symbol, context=context)
            _ctx_add_throttle_wait(request_context, (token or {}).get("wait_ms", 0.0))
            return token
        except Exception as exc:
            log.error(
                "[%s] selector throttle acquire failed endpoint=%s context=%s err=%s",
                symbol, endpoint, context, exc,
            )
            _ctx_note_throttle_issue(
                request_context,
                endpoint=endpoint,
                symbol=symbol,
                context=context,
                phase="acquire",
                error=exc,
            )
            return None

    def _throttle_after_market_data_call(self, token) -> None:
        if token is None:
            return
        try:
            from ap.tradier_market_data_throttle import after_market_data_call
            after_market_data_call()
        except Exception:
            pass

    def _fetch_underlying_quote(
        self,
        session,
        base_url: str,
        headers: dict,
        ticker: str,
        *,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> Optional[float]:
        if request_context is not None and request_context.underlying_price is not None:
            return request_context.underlying_price
        _ctx_assert_budget(request_context, stage="underlying_quote")
        _ctx_increment(request_context, "underlying_quote_calls")
        _t0 = time.monotonic()
        token = self._throttle_before_market_data_call(
            "/v1/markets/quotes",
            ticker,
            context="selector_underlying_quote",
            request_context=request_context,
        )
        underlying_price = None
        try:
            q_resp = session.get(
                f"{base_url}/v1/markets/quotes",
                params={"symbols": ticker, "greeks": "false"},
                headers=headers,
                timeout=8,
            )
            if q_resp.status_code == 200:
                quotes = q_resp.json().get("quotes", {}).get("quote", {})
                if isinstance(quotes, dict):
                    underlying_price = float(quotes.get("last") or quotes.get("bid") or 0) or None
        finally:
            self._throttle_after_market_data_call(token)
            _ctx_add_stage_ms(request_context, "underlying_quote", (time.monotonic() - _t0) * 1000.0)
        if request_context is not None:
            request_context.underlying_price = underlying_price
        return underlying_price

    def _fetch_expirations(
        self,
        session,
        base_url: str,
        headers: dict,
        ticker: str,
        *,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> list[str]:
        if request_context is not None and request_context.expirations is not None:
            return list(request_context.expirations)
        _ctx_assert_budget(
            request_context,
            stage="expirations",
            call_key="expiration_calls",
        )
        _ctx_increment(request_context, "expiration_calls")
        _t0 = time.monotonic()
        token = self._throttle_before_market_data_call(
            "/v1/markets/options/expirations",
            ticker,
            context="selector_expirations",
            request_context=request_context,
        )
        try:
            _provider_t0 = time.monotonic()
            exp_resp = session.get(
                f"{base_url}/v1/markets/options/expirations",
                params={"symbol": ticker, "includeAllRoots": "true"},
                headers=headers,
                timeout=10,
            )
        except Exception as exc:
            _provider_latency_ms = round((time.monotonic() - _provider_t0) * 1000.0, 3)
            if request_context is not None:
                request_context.expiration_provider_latency_ms = _provider_latency_ms
            raise ChainProviderError(
                f"Expirations fetch network error: {exc}",
                provider_latency_ms=_provider_latency_ms,
                attempts=1,
            ) from exc
        finally:
            self._throttle_after_market_data_call(token)
            _ctx_add_stage_ms(request_context, "expirations", (time.monotonic() - _t0) * 1000.0)

        _provider_latency_ms = round((time.monotonic() - _provider_t0) * 1000.0, 3)
        _retry_ms = _retry_after_ms(exp_resp)
        if request_context is not None:
            request_context.expiration_http_status = int(exp_resp.status_code)
            request_context.expiration_provider_latency_ms = _provider_latency_ms
            request_context.retry_after_ms = _retry_ms
            _ctx_refresh_diagnostics(request_context)
        if exp_resp.status_code in (401, 403):
            raise ChainAuthError(
                f"Expirations auth error {exp_resp.status_code} for {ticker}",
                status_code=exp_resp.status_code,
                provider_latency_ms=_provider_latency_ms,
                retry_after_ms=_retry_ms,
            )
        if exp_resp.status_code != 200:
            raise ChainProviderError(
                f"Expirations fetch failed: HTTP {exp_resp.status_code} for {ticker}",
                status_code=exp_resp.status_code,
                provider_latency_ms=_provider_latency_ms,
                retry_after_ms=_retry_ms,
            )
        dates = exp_resp.json().get("expirations", {}).get("date", []) or []
        if request_context is not None:
            request_context.expirations = list(dates)
        return dates

    def _fetch_chain_for_expiration(
        self,
        session,
        base_url: str,
        headers: dict,
        ticker: str,
        option_type: str,
        expiration: str,
        *,
        underlying_price: Optional[float],
        request_context: Optional[SelectorRequestContext] = None,
    ) -> tuple[list[dict], Optional[float]]:
        _ctx_assert_budget(
            request_context,
            stage="chain",
            call_key="chain_calls",
        )
        _ctx_increment(request_context, "chain_calls")
        _t0 = time.monotonic()
        token = self._throttle_before_market_data_call(
            "/v1/markets/options/chains",
            ticker,
            context="selector_chain",
            request_context=request_context,
        )
        try:
            chain_resp = session.get(
                f"{base_url}/v1/markets/options/chains",
                params={"symbol": ticker, "expiration": expiration, "greeks": "true"},
                headers=headers,
                timeout=10,
            )
        except Exception as exc:
            raise ChainProviderError(f"Chain fetch network error (exp={expiration}): {exc}") from exc
        finally:
            self._throttle_after_market_data_call(token)
            _ctx_add_stage_ms(request_context, "chain", (time.monotonic() - _t0) * 1000.0)

        if chain_resp.status_code in (401, 403):
            raise ChainAuthError(
                f"Chain fetch auth error {chain_resp.status_code} for {ticker}/{expiration}",
                status_code=chain_resp.status_code,
            )
        if chain_resp.status_code != 200:
            raise ChainProviderError(
                f"Chain fetch failed: HTTP {chain_resp.status_code} for {ticker}/{expiration}",
                status_code=chain_resp.status_code,
            )

        options = chain_resp.json().get("options", {}).get("option", []) or []
        if not options:
            raise ChainEmptyOptions(
                f"[{ticker}] Tradier returned zero option rows for expiration={expiration}"
            )
        for o in options:
            o["_ticker"] = ticker
            if underlying_price:
                o["_underlying_price"] = underlying_price
        filtered = [o for o in options if o.get("option_type", "").lower() == option_type]
        return filtered, underlying_price

    def _fetch_tradier_chain(
        self,
        ticker: str,
        option_type: str,
        *,
        expiration_override: Optional[str] = None,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> tuple[list[dict], Optional[float]]:
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

        underlying_price = None
        try:
            underlying_price = self._fetch_underlying_quote(
                _session,
                base_url,
                headers,
                ticker,
                request_context=request_context,
            )
        except Exception:
            underlying_price = None

        dates = self._fetch_expirations(
            _session,
            base_url,
            headers,
            ticker,
            request_context=request_context,
        )
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
        return self._fetch_chain_for_expiration(
            _session,
            base_url,
            headers,
            ticker,
            option_type,
            target_exp,
            underlying_price=underlying_price,
            request_context=request_context,
        )

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

    def _fetch_expirations_list(
        self,
        ticker: str,
        *,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> tuple[list[str], dict]:
        """Fetch expirations and return request-scoped provider evidence."""
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
        context = request_context or _new_selector_request_context(ticker)
        dates = self._fetch_expirations(
            _session,
            base_url,
            headers,
            ticker,
            request_context=context,
        )
        evidence = {
            "expiration_fetch_attempts": int(context.provider_call_counts.get("expiration_calls", 0) or 0),
            "expiration_http_status": context.expiration_http_status,
            "expiration_provider_latency_ms": context.expiration_provider_latency_ms,
            "retry_after_ms": context.retry_after_ms,
        }
        if not dates:
            raise ChainEmptyExpirations(
                f"[{ticker}] Tradier returned empty expirations list",
                status_code=context.expiration_http_status,
                provider_latency_ms=context.expiration_provider_latency_ms,
                retry_after_ms=context.retry_after_ms,
                attempts=evidence["expiration_fetch_attempts"] or 1,
            )
        return dates, evidence

    def _set_playbook_audit(
        self,
        plan,
        request_context: Optional[SelectorRequestContext],
        payload: dict,
    ) -> None:
        if request_context is not None:
            request_context.playbook_audit = dict(payload)
            _ctx_refresh_diagnostics(request_context)
        try:
            if isinstance(plan, dict):
                selector_metadata = plan.setdefault("selector_metadata", {})
            else:
                selector_metadata = getattr(plan, "selector_metadata", None)
                if not isinstance(selector_metadata, dict):
                    selector_metadata = {}
                    setattr(plan, "selector_metadata", selector_metadata)
            if isinstance(selector_metadata, dict):
                selector_metadata["playbook"] = dict(payload)
        except Exception:
            pass

    def _plan_expiration_override(self, plan) -> str | None:
        override = _safe_plan_attr(plan, "expiration_override", None)
        if override:
            return str(override)
        metadata = _safe_plan_attr(plan, "metadata", None) or {}
        if isinstance(metadata, dict):
            meta_override = metadata.get("expiration_override")
            if meta_override:
                return str(meta_override)
        return None

    def _resolve_playbook_probe_order(
        self,
        plan,
        ticker: str,
        *,
        request_context: Optional[SelectorRequestContext],
    ) -> tuple[list[str], dict]:
        import requests

        _session = getattr(self.data_broker, "session", None) or requests
        cfg = getattr(self.data_broker, "cfg", None)
        base_url = (
            getattr(cfg, "base_url", None)
            or getattr(self.data_broker, "base_url", "https://sandbox.tradier.com")
        )
        token = (
            getattr(cfg, "access_token", None)
            or getattr(cfg, "token", None)
            or getattr(self.data_broker, "access_token", None)
            or getattr(self.data_broker, "token", "")
        ) or ""
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        underlying_price = self._fetch_underlying_quote(
            _session,
            base_url,
            headers,
            ticker,
            request_context=request_context,
        )
        dates, evidence = self._fetch_expirations_list(
            ticker,
            request_context=request_context,
        )
        now_et = self._playbook_now_et()
        today_et = now_et.date()
        expiration_override = self._plan_expiration_override(plan)
        if request_context is not None:
            request_context.playbook_now_et = now_et
            request_context.playbook_today_et = today_et
            _ctx_refresh_diagnostics(request_context)
        spec = resolve_contract_playbook(
            ticker=ticker,
            side=_safe_plan_attr(plan, "side", ""),
            timeframe=_safe_plan_attr(plan, "timeframe", ""),
            pattern=_safe_plan_attr(plan, "pattern", None),
            underlying_price=float(underlying_price or 0.0),
            trigger_price=_safe_plan_attr(plan, "trigger_price", None),
            target_underlying=_safe_plan_attr(plan, "target_underlying", None),
            wick_targets=_safe_plan_attr(plan, "wick_targets", None) or [],
            available_expirations=dates,
            today=today_et,
            min_dte=int(self.min_dte),
            max_dte=int(self.max_dte),
            metadata=_safe_plan_attr(plan, "metadata", None) or {},
            now_et=now_et,
            expiration_override=expiration_override,
        )
        ordered = resolve_playbook_expiration_order(
            spec,
            dates,
            today=today_et,
            min_dte=int(self.min_dte),
            max_dte=int(self.max_dte),
        )
        if request_context is not None:
            request_context.playbook_spec = spec
            request_context.playbook_ordered_expirations = list(ordered)
        audit = {
            "enabled": True,
            "policy_version": "pr303-amendment-v1",
            "instrument_class": spec.instrument_class,
            "timeframe": spec.timeframe,
            "available_expirations": list(dates),
            "ordered_expirations": list(ordered),
            "expirations_probed": [],
            "preferred_dte_order": list(spec.preferred_dte_order),
            "selected_expiration": None,
            "selected_dte": None,
            "expiration_fallback_used": False,
            "expiration_fallback_reason": None,
            "underlying_price": spec.underlying_price,
            "trigger_price": spec.trigger_price,
            "target_underlying": spec.target_underlying,
            "atm_strike": None,
            "one_step_otm_strike": None,
            "preferred_strikes": list(spec.preferred_strikes),
            "strike_band_low": spec.strike_band_low,
            "strike_band_high": spec.strike_band_high,
            "selected_strike": None,
            "selected_initial_rank": None,
            "selected_strike_policy_tier": None,
            "selected_strike_policy_label": None,
            "selected_original_rank_score": None,
            "selection_reason": None,
            "policy_reason": spec.policy_reason,
            "diagnostics": dict(spec.diagnostics),
        }
        audit.update(evidence)
        audit["resolved_now_et"] = now_et.isoformat()
        audit["resolved_today_et"] = today_et.isoformat()
        return ordered, audit

    def _playbook_now_et(self) -> datetime:
        return datetime.now(ZoneInfo("America/New_York"))

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

    def _select_with_dte_ladder(
        self,
        plan,
        *,
        request_context: Optional[SelectorRequestContext] = None,
    ) -> Optional["SelectedContract"]:
        """Evaluate expirations by DTE bucket in playbook-preferred order.

        For each bucket (A→B→C by default), probe its expirations nearest-first
        (up to dte_ladder_probe_per_bucket) and return the FIRST expiration that
        yields a quality survivor. Quality gates are unchanged — this only
        controls which expiration select() evaluates. Records a full audit of
        buckets attempted / survivors per bucket for diagnostics.

        Returns the selected contract, or None (records NO_VALID_PLAYBOOK_DTE_CONTRACT).
        """
        ticker = _safe_plan_attr(plan, "ticker")
        if request_context is None and playbook_contract_selection_enabled():
            request_context = _new_selector_request_context(
                ticker,
                getattr(self, "mode", "unknown"),
            )
            _bind_selector_request_diagnostics(plan, request_context)
        if request_context is not None:
            _request_playbook_enabled(request_context)
        audit: dict = {
            "ladder": True,
            "ticker": ticker,
            "timeframe": str(_safe_plan_attr(plan, "timeframe", "") or ""),
            "bucket_order": [],
            "buckets_attempted": [],
            "selected_bucket": None,
            "selected_dte": None,
            "selected_expiration": None,
            "expiration_fetch_attempts": 0,
            "expiration_http_status": None,
            "expiration_provider_latency_ms": None,
            "retry_after_ms": None,
            "final_reason": None,
            "fallback_considered": False,
            "fallback_allowed": False,
            "fallback_used": False,
            "fallback_succeeded": False,
        }

        def _persist(final_reason: str | None = None) -> None:
            if final_reason is not None:
                audit["final_reason"] = final_reason
            audit["selection_diagnostics"] = _selector_request_diagnostics(request_context)
            _persist_dte_ladder_audit(self, plan, audit)

        def _ladder_selector_failure_meta(
            reason_code: str,
            *,
            stage: str,
            explanation: str,
            execution_mode: str,
        ) -> dict:
            """Build selector-owned truth for ladder paths that bypass select().

            Expiration-fetch and no-expiration ladder exits do not pass through
            the normal ``_attach_selector_failure`` seam. Keep their durable
            metadata shape identical so execution core never has to infer
            canonical truth from a ladder exception or a stale loop reason.
            """
            _reason = str(reason_code or "UNKNOWN_REJECTION").strip() or "UNKNOWN_REJECTION"
            _operational = (
                _reason
                if _reason in {
                    "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "MARKET_DATA_THROTTLE_UNAVAILABLE",
                }
                else None
            )
            return {
                "stage": str(stage or "dte_ladder"),
                "reason_code": _reason,
                "canonical_selector_reason": _reason,
                "last_observed_selector_reason": _reason,
                "selector_terminal_reason": _reason,
                "operational_reason": _operational,
                "queue_reason_code": _to_queue_reason(_reason),
                "explanation": str(explanation or ""),
                "chain_rows": 0,
                "survivor_count": 0,
                "execution_mode": str(execution_mode or "unknown").lower(),
            }

        def _finalize_ladder_failure(failure: dict | None) -> dict:
            """Restore the complete selector-owned failure shape after probing."""
            _final = dict(failure or {})
            _reason = _dte_failure_reason(_final)
            _final.setdefault("stage", "dte_ladder")
            _final.setdefault("reason_code", _reason)
            _final.setdefault("canonical_selector_reason", _reason)
            _final.setdefault(
                "last_observed_selector_reason",
                str(_final.get("reason_code") or _reason),
            )
            _final.setdefault("selector_terminal_reason", _reason)
            _final.setdefault("operational_reason", None)
            _final.setdefault("queue_reason_code", _to_queue_reason(_reason))
            self._set_last_failure(_final)
            _restore_selector_failure(plan, _final)
            return _final

        def _expiration_failure(reason_code: str, exc: Exception, *, allow_fallback: bool):
            audit.update({
                "expiration_fetch_attempts": int(getattr(exc, "attempts", 1) or 1),
                "expiration_http_status": getattr(exc, "status_code", None),
                "expiration_provider_latency_ms": getattr(exc, "provider_latency_ms", None),
                "retry_after_ms": getattr(exc, "retry_after_ms", None),
                "final_reason": reason_code,
                "fallback_considered": bool(allow_fallback),
            })
            _mode = str(
                _safe_plan_attr(plan, "execution_mode", None)
                or getattr(self, "mode", "unknown")
                or "unknown"
            ).strip().upper()
            _fallback_allowed = bool(
                allow_fallback
                and (_mode == "PAPER" or getattr(self, "deferred_dte_legacy_fallback", False))
            )
            audit["fallback_allowed"] = _fallback_allowed
            audit["fallback_policy"] = "paper" if _mode == "PAPER" else "explicit_env"
            _failure = _ladder_selector_failure_meta(
                reason_code,
                stage="dte_ladder_expirations",
                explanation=str(exc),
                execution_mode=_mode,
            )
            audit["failure"] = dict(_failure)
            self._set_last_failure(_failure)
            _failure_meta = dict(_failure)
            _restore_selector_failure(plan, _failure_meta)

            if _fallback_allowed:
                audit["fallback_used"] = True
                if request_context is not None:
                    request_context.legacy_fallback_used = True
                    if request_context.expirations == []:
                        request_context.expirations = None
                log.warning(
                    "[%s] DTE_LADDER_LEGACY_FALLBACK reason=%s mode=%s explicit_env=%s",
                    ticker, reason_code, _mode,
                    bool(getattr(self, "deferred_dte_legacy_fallback", False)),
                )
                try:
                    result = self.select(
                        plan,
                        _dte_legacy_fallback=True,
                        request_context=request_context,
                    )
                except Exception as fallback_exc:
                    audit["fallback_error"] = str(fallback_exc)
                    result = None
                if request_context is not None:
                    audit.update({
                        "expiration_fetch_attempts": int(
                            request_context.provider_call_counts.get("expiration_calls", 0) or 0
                        ),
                        "expiration_http_status": request_context.expiration_http_status,
                        "expiration_provider_latency_ms": request_context.expiration_provider_latency_ms,
                        "retry_after_ms": request_context.retry_after_ms,
                    })
                audit["fallback_succeeded"] = result is not None
                if result is not None:
                    _persist(reason_code)
                    return result
                self._set_last_failure(_failure)
                _restore_selector_failure(plan, _failure_meta)

            _persist(reason_code)
            return None

        try:
            playbook_audit: dict | None = None
            try:
                if bool(getattr(request_context, "playbook_enabled", False)):
                    dates, playbook_audit = self._resolve_playbook_probe_order(
                        plan,
                        ticker,
                        request_context=request_context,
                    )
                    audit["playbook_enabled"] = True
                    audit["playbook_ordered_expirations"] = list(dates)
                    if playbook_audit is not None:
                        self._set_playbook_audit(plan, request_context, playbook_audit)
                else:
                    _fetch_result = self._fetch_expirations_list(
                        ticker,
                        request_context=request_context,
                    )
                    if (
                        isinstance(_fetch_result, tuple)
                        and len(_fetch_result) == 2
                        and isinstance(_fetch_result[1], dict)
                    ):
                        dates, _fetch_evidence = _fetch_result
                        audit.update(_fetch_evidence)
                    else:
                        dates = _fetch_result
                        audit["expiration_fetch_attempts"] = 1
            except ChainAuthError as exc:
                return _expiration_failure("CHAIN_AUTH_ERROR", exc, allow_fallback=False)
            except SelectorRequestBudgetExhausted as exc:
                return _expiration_failure(
                    "SELECTOR_REQUEST_BUDGET_EXHAUSTED", exc, allow_fallback=False
                )
            except ChainEmptyExpirations as exc:
                return _expiration_failure(
                    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS", exc, allow_fallback=True
                )
            except ChainProviderError as exc:
                return _expiration_failure("CHAIN_PROVIDER_ERROR", exc, allow_fallback=True)

            today = getattr(request_context, "playbook_today_et", None) or date.today()

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
            }
            _OPERATIONAL_STOP_REASONS = {
                "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                "MARKET_DATA_THROTTLE_UNAVAILABLE",
            }
            # P0 amendment: the DTE ladder previously maintained its own
            # handwritten _RETRYABLE_DATA_REASONS set, independent of the
            # authoritative ap/selector_retry_policy.py policy table. A new
            # RETRYABLE_DATA reason added to that table (e.g.
            # DUPLICATE_QUOTE_CONFLICT_UNRESOLVED) silently fell through to
            # the generic quality branch below instead of the retryable-data
            # preservation branch, because this local set was never updated.
            # That produced two conflicting authorities for the same reason
            # code depending on which layer consumed it. Deriving directly
            # from the shared policy makes this structurally impossible to
            # repeat: any future RETRYABLE_DATA reason is automatically
            # recognized here with no separate list to maintain or forget.
            # _TERMINAL_NON_DTE and _OPERATIONAL_STOP_REASONS above are
            # intentionally still DTE-ladder-specific scoping and are
            # unaffected -- both checks intercept and return before this
            # classification is ever reached, so this change cannot alter
            # their behavior.
            from ap.selector_retry_policy import is_retryable_selector_reason

            _preserved_terminal = None
            _preserved_retryable = None
            _preserved_quality = None

            if bool(getattr(request_context, "playbook_enabled", False)):
                ordered_expirations = list(dates)
                if not ordered_expirations:
                    _playbook_reason = None
                    _playbook_explanation = None
                    if isinstance(playbook_audit, dict):
                        _playbook_reason = ((playbook_audit.get("diagnostics") or {}).get("error_reason"))
                        _playbook_explanation = ((playbook_audit.get("diagnostics") or {}).get("error_explanation"))
                    _reason_code = _playbook_reason or "PLAYBOOK_NO_POLICY_MATCH"
                    _no_policy_failure = _ladder_selector_failure_meta(
                        _reason_code,
                        stage="dte_ladder",
                        explanation=(
                            _playbook_explanation
                            or "Playbook produced no approved expiration inside the DTE window"
                        ),
                        execution_mode=(
                            _safe_plan_attr(plan, "execution_mode", None)
                            or getattr(self, "mode", "unknown")
                        ),
                    )
                    self._set_last_failure(_no_policy_failure)
                    _restore_selector_failure(plan, _no_policy_failure)
                    if playbook_audit is not None:
                        playbook_audit["selection_reason"] = _reason_code
                        self._set_playbook_audit(plan, request_context, playbook_audit)
                    _persist(_reason_code)
                    return None
                probe_cap = max(1, self.dte_ladder_probe_per_bucket * 3)
                buckets = {"PLAYBOOK": ordered_expirations[:probe_cap]}
                order = ["PLAYBOOK"]
                audit["bucket_order"] = order
                audit["playbook_probe_cap"] = probe_cap
            else:
                buckets = self._bucket_expirations(dates)
                order = self._preferred_bucket_order(plan)
                audit["bucket_order"] = order

            for bucket_name in order:
                exps = buckets.get(bucket_name, [])[: self.dte_ladder_probe_per_bucket] if bucket_name != "PLAYBOOK" else buckets.get(bucket_name, [])
                bucket_rec = {"bucket": bucket_name, "expirations_probed": [], "survivor": False}
                for exp in exps:
                    if request_context is not None:
                        request_context.expirations_probed.append(exp)
                    if playbook_audit is not None:
                        playbook_audit["expirations_probed"].append(exp)
                        playbook_audit["expiration_fallback_used"] = bool(playbook_audit["expirations_probed"][:-1])
                        if playbook_audit["expiration_fallback_used"] and playbook_audit["expiration_fallback_reason"] is None:
                            playbook_audit["expiration_fallback_reason"] = "preferred_expiration_rejected"
                    try:
                        _dte = (date.fromisoformat(exp) - today).days
                    except Exception:
                        _dte = None
                    result = self.select(
                        plan,
                        expiration_override=exp,
                        request_context=request_context,
                    )
                    _sub_fail = _get_selector_failure(plan)
                    if result is None and isinstance(_sub_fail, dict):
                        _reason_code = _dte_failure_reason(_sub_fail)
                        if _reason_code in _OPERATIONAL_STOP_REASONS:
                            # A request-budget/throttle stop curtails further
                            # provider work, but it is not allowed to replace
                            # a stronger selector verdict already observed in
                            # an earlier expiration.
                            bucket_rec["expirations_probed"].append({
                                "exp": exp, "dte": _dte, "hit": False,
                                "failure": dict(_sub_fail),
                            })
                            audit["buckets_attempted"].append(bucket_rec)
                            _prior_failure = (
                                _preserved_terminal
                                or _preserved_quality
                                or _preserved_retryable
                            )
                            _final_failure = (
                                _merge_dte_operational_failure(
                                    _prior_failure,
                                    _sub_fail,
                                )
                                if _prior_failure is not None
                                else dict(_sub_fail)
                            )
                            _finalize_ladder_failure(_final_failure)
                            if playbook_audit is not None:
                                playbook_audit["selection_reason"] = _dte_failure_reason(
                                    _final_failure
                                )
                                self._set_playbook_audit(
                                    plan, request_context, playbook_audit
                                )
                            _persist(_dte_failure_reason(_final_failure))
                            log.warning(
                                "[%s] DTE_LADDER_OPERATIONAL_STOP observed=%s canonical=%s ",
                                ticker,
                                _reason_code,
                                _dte_failure_reason(_final_failure),
                            )
                            return None
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
                            _finalize_ladder_failure(_preserved_terminal)
                            _persist(_dte_failure_reason(_preserved_terminal))
                            log.warning(
                                "[%s] DTE_LADDER_TERMINAL_NON_DTE preserved reason=%s "
                                "— stopping ladder immediately (non-DTE verdict)",
                                ticker, _preserved_terminal.get("reason_code"),
                            )
                            return None
                        elif is_retryable_selector_reason(_reason_code):
                            # Data miss — keep probing, remember this as a candidate
                            # for preservation if no quality reason appears.
                            if _preserved_retryable is None:
                                _preserved_retryable = dict(_sub_fail)
                        elif _reason_code:
                            # Quality reject (OI_TOO_LOW, SPREAD_TOO_WIDE, etc.)
                            # Keep probing — a different expiration might pass.
                            # Reduce by fixed policy precedence rather than
                            # letting the last expiration overwrite earlier
                            # economic/quality truth.
                            _preserved_quality = _choose_stronger_dte_quality_failure(
                                _preserved_quality,
                                _sub_fail,
                            )
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
                        if playbook_audit is not None:
                            playbook_audit["selected_expiration"] = exp
                            playbook_audit["selected_dte"] = _dte
                            playbook_audit["selected_strike"] = _safe_float(getattr(result, "strike", None))
                            playbook_audit["selection_reason"] = "highest_ranked_fully_eligible_playbook_candidate"
                            self._set_playbook_audit(plan, request_context, playbook_audit)
                        _persist("SELECTED")
                        log.info(
                            "[%s] DTE_LADDER_SELECTED bucket=%s dte=%s exp=%s",
                            ticker, bucket_name, _dte, exp,
                        )
                        return result
                audit["buckets_attempted"].append(bucket_rec)

            # All buckets exhausted. Priority for final reason preservation:
            #   1. Quality reject from a usable chain — preserve the truthful
            #      blocker rather than an earlier transient data miss.
            #   2. Retryable data-miss — only when every probed expiration stayed
            #      transient and no later usable-chain quality verdict occurred.
            #   3. NO_VALID_PLAYBOOK_DTE_CONTRACT — fallback when no specific reason
            #      was captured (empty chain, ladder never got any sub-failure).
            #
            # NOTE: _preserved_terminal is handled above via early return; it
            # should be None here.
            if _preserved_quality is not None:
                _finalize_ladder_failure(_preserved_quality)
                if playbook_audit is not None:
                    playbook_audit["selection_reason"] = _dte_failure_reason(
                        _preserved_quality
                    )
                    self._set_playbook_audit(plan, request_context, playbook_audit)
                _persist(_dte_failure_reason(_preserved_quality))
                log.warning(
                    "[%s] DTE_LADDER_QUALITY_REASON preserved reason=%s "
                    "— not masking with NO_VALID_PLAYBOOK_DTE_CONTRACT",
                    ticker, _dte_failure_reason(_preserved_quality),
                )
                return None
            if _preserved_retryable is not None:
                _finalize_ladder_failure(_preserved_retryable)
                if playbook_audit is not None:
                    playbook_audit["selection_reason"] = _dte_failure_reason(
                        _preserved_retryable
                    )
                    self._set_playbook_audit(plan, request_context, playbook_audit)
                _persist(_dte_failure_reason(_preserved_retryable))
                log.warning(
                    "[%s] DTE_LADDER_RETRYABLE_REASON preserved reason=%s "
                    "— not masking with NO_VALID_PLAYBOOK_DTE_CONTRACT",
                    ticker, _dte_failure_reason(_preserved_retryable),
                )
                return None
            _final_reason = "PLAYBOOK_NO_FULLY_ELIGIBLE_CONTRACT" if bool(getattr(request_context, "playbook_enabled", False)) else "NO_VALID_PLAYBOOK_DTE_CONTRACT"
            _no_survivor_failure = _ladder_selector_failure_meta(
                _final_reason,
                stage="dte_ladder",
                explanation=(
                    "No quality survivor in any evaluated DTE bucket "
                    f"(order={order}, buckets={ {k: len(v) for k, v in buckets.items()} })"
                ),
                execution_mode=(
                    _safe_plan_attr(plan, "execution_mode", None)
                    or getattr(self, "mode", "unknown")
                ),
            )
            self._set_last_failure(_no_survivor_failure)
            _restore_selector_failure(plan, _no_survivor_failure)
            log.warning(
                "[%s] DTE_LADDER_NO_SURVIVOR order=%s buckets=%s",
                ticker, order, {k: len(v) for k, v in buckets.items()},
            )
            if playbook_audit is not None:
                playbook_audit["selection_reason"] = _final_reason
                self._set_playbook_audit(plan, request_context, playbook_audit)
            _persist(_final_reason)
            return None
        except Exception as exc:
            log.warning("[%s] DTE_LADDER_ERROR fail-closed: %s", ticker, exc)
            audit["failure"] = {
                "stage": "dte_ladder",
                "reason_code": "DTE_LADDER_ERROR",
                "explanation": str(exc),
            }
            _ladder_error_failure = _ladder_selector_failure_meta(
                "DTE_LADDER_ERROR",
                stage="dte_ladder",
                explanation=str(exc),
                execution_mode=str(getattr(self, "mode", "unknown") or "unknown"),
            )
            self._set_last_failure(_ladder_error_failure)
            _restore_selector_failure(plan, _ladder_error_failure)
            _persist("DTE_LADDER_ERROR")
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
        assert _normalized_selector_mode(self.mode) != "LIVE", (
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
        is_live, _pricing_basis = _pricing_basis_for_mode(self.mode)
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
            is_live, pricing_basis = _pricing_basis_for_mode(self.mode)
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

            # OCC identity is canonical at the selection boundary as well as
            # at direct-quote/revalidation boundaries. This prevents a
            # whitespace/case variant from escaping duplicate normalization
            # into the plan or broker-facing contract field.
            contract_symbol = "".join(
                str(opt.get("symbol") or opt.get("contract") or "").upper().split()
            )
            return SelectedContract(
                contract_symbol      = contract_symbol,
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
