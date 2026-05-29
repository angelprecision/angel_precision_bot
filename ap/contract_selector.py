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
    "zero_bid_or_ask":       "NO_CHAIN_DATA",
    "ask_below_bid":         "NO_CHAIN_DATA",
    "zero_mid":              "NO_CHAIN_DATA",
    "size_too_thin":         "VOLUME_TOO_LOW",
    "spread_too_wide":       "SPREAD_TOO_WIDE",
    "illiquid_vol":          "OI_TOO_LOW",
    "bid_below":             "NO_AFFORDABLE_CONTRACT",
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


def _build_candidate_audit(scored, underlying_price, selected_symbol, selected_reason, rejections, top_n=3):
    """Item 3 — build the persisted selector candidate audit (EVIDENCE ONLY).

    scored: list of (rank_score, opt_dict) already sorted best-first.
    Returns a dict safe to drop into orders.meta. Never raises.
    """
    candidates = []
    try:
        for _rank, (_s, _c) in enumerate(scored[:top_n]):
            bid = _safe_float(_c.get("bid"))
            ask = _safe_float(_c.get("ask"))
            mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else _safe_float(_c.get("mid"))
            last = _safe_float(_c.get("last"))
            spread_pct = ((ask - bid) / mid) if mid > 0 else None
            abs_delta, delta_reason = _extract_abs_delta(_c)
            strike = _safe_float(_c.get("strike"))
            up = _safe_float(underlying_price)
            moneyness_pct = ((strike - up) / up) if up > 0 else None
            candidates.append({
                "rank":                _rank + 1,
                "contract":            _c.get("symbol", "?"),
                "strike":              strike,
                "expiration":          _c.get("expiration_date") or _c.get("expiration") or "",
                "dte":                 _c.get("dte"),
                "bid":                 round(bid, 4),
                "ask":                 round(ask, 4),
                "mid":                 round(mid, 4),
                "last":                round(last, 4),
                "spread_pct":          round(spread_pct, 4) if spread_pct is not None else None,
                "delta":               round(abs_delta, 4) if abs_delta is not None else None,
                "delta_reason":        delta_reason,
                "moneyness_pct":       round(moneyness_pct, 4) if moneyness_pct is not None else None,
                "distance_from_underlying": round(strike - up, 4) if up > 0 else None,
                "volume":              int(_safe_float(_c.get("volume"))),
                "open_interest":       int(_safe_float(_c.get("open_interest"))),
                "rank_score":          round(_safe_float(_s), 4),
            })
    except Exception:
        pass

    rejected = {}
    try:
        rejected = dict(sorted((rejections or {}).items(), key=lambda x: -x[1]))
    except Exception:
        rejected = {}

    return {
        "selected_contract":          selected_symbol,
        "selected_reason":            selected_reason,
        "underlying_price":           round(_safe_float(underlying_price), 4),
        "candidates_considered":      len(scored) if scored else 0,
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





from ap.observability import emit_decision_event, get_git_commit, make_config_hash
log = logging.getLogger("ap.contract_selector")


# =============================================================================
# PRO-LEVEL CONTRACT QUALITY THRESHOLDS
# =============================================================================

_PRO_QUALITY_ENABLED = os.getenv("PRO_CONTRACT_QUALITY", "true").lower() != "false"

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

    if bid_size or ask_size:
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
    # PUBLIC -- select(plan) → SelectedContract | None
    # =========================================================================


    def select(self, plan) -> Optional[SelectedContract]:
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

        # ── A. FETCH CHAIN ────────────────────────────────────────────────────
        try:
            chain, underlying_price = self._fetch_chain_with_price(ticker, direction)
        except Exception as e:
            log.error("[%s] chain fetch failed: %s", ticker, e)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="NO_CHAIN_DATA",
                explanation=f"Chain fetch failed: {e}",
                inputs={"ticker": ticker, "direction": direction},
            )
            return None

        if not chain:
            log.warning("[%s] EMPTY CHAIN -- Tradier returned no options", ticker)
            self._emit_selector_event(
                plan,
                stage="chain_fetch",
                decision="REJECT",
                reason_code="NO_CHAIN_DATA",
                explanation="Tradier returned empty options chain",
                inputs={"ticker": ticker, "direction": direction},
            )
            return None

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

        # ── B. HARD QUALITY FILTER ────────────────────────────────────────────
        # No self-mutation: effective thresholds passed directly to _quality_filter.
        # This is thread-safe when worker_loop and entry_watcher breach both call
        # select() on the same instance simultaneously.
        today      = date.today()
        survivors  = []
        _rejections: dict = {}
        _pro_tiers:  dict = {"A": 0, "B": 0}

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
            if result is None:
                survivors.append(opt)
            else:
                _rejections[result] = _rejections.get(result, 0) + 1
                log.debug("[%s] filtered: %s -- %s", ticker, opt.get("symbol", "?"), result)
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
                },
                thresholds={
                    "max_spread_pct": _safe_float(_eff_max_spread),
                    "min_oi":         _safe_float(_eff_min_oi),
                },
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
                )
                if not _allow_cheap:
                    return None
        selected = self._build_selected(best, best_score, budget, today)
        if selected is None:
            return None

        # Item 3 — attach the selector candidate audit (EVIDENCE ONLY). Built
        # from the final sorted `scored` list + the rejection counts. Persisted
        # downstream into orders.meta. Never affects selection. Best-effort.
        try:
            selected.candidate_audit = _build_candidate_audit(
                scored,
                underlying_price or 0.0,
                selected_symbol=selected.contract_symbol,
                selected_reason=selected.selection_reason,
                rejections=_rejections,
                top_n=int(os.getenv("SELECTOR_CANDIDATE_AUDIT_TOP_N", "3")),
            )
        except Exception:
            selected.candidate_audit = None

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
                        context={"mode": self.mode},
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
                )
            else:
                log.warning("[%s] BLOCKED -- budget $%.0f cannot afford %s @ $%.0f/contract",
                            ticker, budget, selected.contract_symbol, selected.premium_per_contract)
                self._emit_selector_event(
                    plan,
                stage="affordability_gate",
                    decision="REJECT",
                    reason_code="NO_AFFORDABLE_CONTRACT",
                    explanation=(
                        f"Budget ${budget:.0f} cannot afford "
                        f"{selected.contract_symbol} @ ${selected.premium_per_contract:.0f}/contract"
                    ),
                    contract=selected.contract_symbol,
                    inputs={
                        "budget":               _safe_float(budget),
                        "premium_per_contract": _safe_float(selected.premium_per_contract),
                        "affordable_contracts": selected.affordable_contracts,
                    },
                    thresholds={"min_contracts": 1},
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

    def _fetch_chain_with_price(self, ticker: str, direction: str) -> tuple[list[dict], Optional[float]]:
        option_type = direction.lower()
        return self._fetch_tradier_chain(ticker, option_type)

    def _fetch_chain(self, ticker: str, direction: str) -> list[dict]:
        chain, _ = self._fetch_chain_with_price(ticker, direction)
        return chain

    def _fetch_tradier_chain(self, ticker: str, option_type: str) -> tuple[list[dict], Optional[float]]:
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
            q_resp = _session.get(
                f"{base_url}/v1/markets/quotes",
                params={"symbols": ticker, "greeks": "false"},
                headers=headers, timeout=8,
            )
            if q_resp.status_code == 200:
                quotes = q_resp.json().get("quotes", {}).get("quote", {})
                if isinstance(quotes, dict):
                    underlying_price = float(quotes.get("last") or quotes.get("bid") or 0) or None
        except Exception:
            pass

        # 2. Get expirations
        try:
            exp_resp = _session.get(
                f"{base_url}/v1/markets/options/expirations",
                params={"symbol": ticker, "includeAllRoots": "true"},
                headers=headers, timeout=10,
            )
        except requests.exceptions.RequestException as _e:
            raise ValueError(f"Expirations fetch network error: {_e}") from _e
        if exp_resp.status_code != 200:
            raise ValueError(f"Expirations fetch failed: {exp_resp.status_code}")

        dates = exp_resp.json().get("expirations", {}).get("date", []) or []
        if not dates:
            return [], underlying_price

        # 3. Pick best expiration
        target_exp = self._pick_expiration(dates)
        if not target_exp:
            return [], underlying_price

        # 4. Get chain with greeks
        try:
            chain_resp = _session.get(
                f"{base_url}/v1/markets/options/chains",
                params={"symbol": ticker, "expiration": target_exp, "greeks": "true"},
                headers=headers, timeout=10,
            )
        except requests.exceptions.RequestException as _e:
            raise ValueError(f"Chain fetch network error: {_e}") from _e
        if chain_resp.status_code != 200:
            raise ValueError(f"Chain fetch failed: {chain_resp.status_code}")

        options = chain_resp.json().get("options", {}).get("option", []) or []

        # 5. Inject ticker into every option dict regardless of quote availability.
        # _quality_filter() uses _ticker for per-ticker premium caps, so this must
        # not depend on underlying_price being present. Inject underlying only when available.
        for o in options:
            o["_ticker"] = ticker
            if underlying_price:
                o["_underlying_price"] = underlying_price

        filtered = [o for o in options if o.get("option_type", "").lower() == option_type]
        return filtered, underlying_price

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
