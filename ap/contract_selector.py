# ap/contract_selector.py
#
# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL QUALITY GATE INVARIANT (do not remove)
# ══════════════════════════════════════════════════════════════════════════════
# contract_selector is the sole contract-quality authority for the queue path:
#   /signal → queue → worker_loop → master_control → contract_selector → watcher
#
# The breach path (_on_entry_trigger) uses evaluate_contract() from
# ap_options_intelligence.py.  These two gates MUST remain equivalent or
# contract_selector must be stricter.
#
# RULE: Any future change to evaluate_contract() quality thresholds (IV caps,
# spread limits, liquidity floors, premium bounds) MUST be mirrored here, OR
# both paths must import from a shared rules module.
#
# Reclassified Bug 8 (audit 2026-04-24) as architectural note — not a defect —
# because contract_selector already enforces equivalent quality controls.
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
    return raw_reason.upper()


def _safe_float(val, default: float = 0.0) -> float:
    """Coerce any value to float safely — never raises."""
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


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

_PRO_TIER1_TICKERS = {
    "SPY", "QQQ", "IWM", "DIA",
    "AAPL", "MSFT", "NVDA", "AMD", "META", "GOOG", "GOOGL",
    "TSLA", "AMZN", "NFLX",
}

_PRO_MIN_BID            = 0.10
_PRO_MIN_BID_SIZE_HARD  = 3

_PRO_T1_SPREAD_HARD_MAX = 0.06
_PRO_T1_SPREAD_A_TIER   = 0.03
_PRO_T1_SIZE_MIN        = 10

_PRO_T2_SPREAD_HARD_MAX = 0.08
_PRO_T2_SPREAD_A_TIER   = 0.04
_PRO_T2_SIZE_MIN        = 5


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
        min_vol, min_oi = 100, 500
    elif dte <= 7:
        min_vol, min_oi = 200, 1000
    else:
        min_vol, min_oi = 500, 2000

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

        log.info("[%s] ContractSelector | direction=%s budget=$%.0f tier=%s pt1=%s",
                 ticker, direction, budget, plan.tier, pt1)

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
                log.warning("[%s] EarningsGuard raised unexpectedly (%s) -- continuing (fail open)",
                            ticker, exc)
                self._emit_selector_event(
                    plan,
                    stage="earnings_gate",
                    decision="ERROR",
                    reason_code="EARNINGS_GUARD_ERROR",
                    explanation=f"EarningsGuard exception; selector continued fail-open: {exc}",
                    inputs={"ticker": ticker},
                    context={"fail_open": True},
                )

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
                log.warning("[%s] IVRankFilter raised unexpectedly (%s) -- continuing (fail open)",
                            ticker, exc)
                self._emit_selector_event(
                    plan,
                    stage="iv_gate",
                    decision="ERROR",
                    reason_code="IV_FILTER_ERROR",
                    explanation=f"IVRankFilter exception; selector continued fail-open: {exc}",
                    inputs={"ticker": ticker, "underlying_price": _safe_float(underlying_price)},
                    context={"fail_open": True},
                )

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
        _orig_spread        = self.max_spread_pct
        _orig_oi            = self.min_oi
        _orig_vol           = self.min_volume
        try:
            self.max_spread_pct = _eff_max_spread
            self.min_oi         = _eff_min_oi
            self.min_volume     = _eff_min_volume

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

                result = self._quality_filter(opt, today)
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

        finally:
            self.max_spread_pct = _orig_spread
            self.min_oi         = _orig_oi
            self.min_volume     = _orig_vol

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

        # ── D. BUILD SELECTED CONTRACT ────────────────────────────────────────
        selected = self._build_selected(best, best_score, budget, today)
        if selected is None:
            return None

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
        if isinstance(plan, dict):
            plan["contract_symbol"] = selected.contract_symbol
            plan["limit_price"]     = selected.ask
            plan["contracts"]       = selected.affordable_contracts
            plan["max_position_usd"] = plan["contracts"] * selected.premium_per_contract
        else:
            plan.contract_symbol  = selected.contract_symbol
            plan.limit_price      = selected.ask
            plan.contracts        = selected.affordable_contracts
            plan.max_position_usd = plan.contracts * selected.premium_per_contract
        if _wick_confidence and not getattr(plan, "wick_confidence", None):
            try:
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
                "simulation_override": "[forced_1]" in (selected.selection_reason or ""),
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
            q_resp = requests.get(
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
        exp_resp = requests.get(
            f"{base_url}/v1/markets/options/expirations",
            params={"symbol": ticker, "includeAllRoots": "true"},
            headers=headers, timeout=10,
        )
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
        chain_resp = requests.get(
            f"{base_url}/v1/markets/options/chains",
            params={"symbol": ticker, "expiration": target_exp, "greeks": "true"},
            headers=headers, timeout=10,
        )
        if chain_resp.status_code != 200:
            raise ValueError(f"Chain fetch failed: {chain_resp.status_code}")

        options = chain_resp.json().get("options", {}).get("option", []) or []

        # 5. Inject underlying price AND ticker into every option dict
        # CRITICAL: both injections must be INSIDE the for loop so every
        # option gets them — not just the last one
        if underlying_price:
            for o in options:
                o["_underlying_price"] = underlying_price
                o["_ticker"] = ticker          # ← INSIDE loop (was outside = bug)

        filtered = [o for o in options if o.get("option_type", "").lower() == option_type]
        return filtered, underlying_price

    def _pick_expiration(self, dates: list[str]) -> Optional[str]:
        today = date.today()
        valid = []

        for d_str in dates:
            try:
                d = date.fromisoformat(d_str)
                dte = (d - today).days
                if self.min_dte <= dte <= self.max_dte:
                    valid.append((dte, d_str))
            except Exception:
                continue

        if not valid:
            for d_str in dates:
                try:
                    d = date.fromisoformat(d_str)
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
        log.warning("[%s] SYNTHETIC CONTRACT -- %s", ticker, reason)
        return SelectedContract(
            contract_symbol=f"{ticker}_SIM", expiration="SIM", strike=0,
            option_type=direction.lower(), bid=0.50, ask=1.00, mid=0.75,
            spread_pct=0.20, delta=0.50, open_interest=1, volume=1,
            premium_per_share=0.75, premium_per_contract=75.0,
            affordable_contracts=1, selection_reason=reason,
            selection_score=0, dte=0,
        )

    def _quality_filter(self, opt: dict, today: date) -> Optional[str]:
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
        if spread_pct > self.max_spread_pct:
            return "spread_too_wide_%.1f%%" % (spread_pct * 100)

        if oi < self.min_oi:
            return "low_oi_%d" % oi
        if vol < self.min_volume:
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
        delta  = greeks.get("delta")
        if delta is not None:
            try:
                delta = abs(float(delta))
                min_d = max(0.05, self.target_delta - self.delta_band)
                max_d = min(0.95, self.target_delta + self.delta_band)
                if delta < min_d or delta > max_d:
                    return "delta_out_of_band_%.2f" % delta
            except Exception:
                pass
        else:
            underlying_price = opt.get("_underlying_price")
            strike = float(opt.get("strike") or 0)
            if underlying_price and strike:
                moneyness   = strike / float(underlying_price)
                option_type = opt.get("option_type", "").lower()
                if option_type == "call" and not (0.93 <= moneyness <= 1.12):
                    return "moneyness_out_of_range_%.3f" % moneyness
                if option_type == "put" and not (0.88 <= moneyness <= 1.07):
                    return "moneyness_out_of_range_%.3f" % moneyness

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
        premium    = mid * 100

        greeks = opt.get("greeks") or {}
        try:
            delta = abs(float(greeks.get("delta") or self.target_delta))
        except Exception:
            delta = self.target_delta

        MAX_IDEAL_PREMIUM = float(os.getenv("MAX_IDEAL_PREMIUM", "3.50"))
        premium_penalty   = max(0.0, mid - MAX_IDEAL_PREMIUM)

        effective_budget = min(budget, float(os.getenv("MAX_TRADE_USD", "500")))
        affordable = int(effective_budget / premium) if premium > 0 else 0

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
            (premium_penalty * premium_weight) +
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

            premium_per_share    = mid
            premium_per_contract = mid * 100
            MAX_TRADE_USD        = float(os.getenv("MAX_TRADE_USD", "500"))
            effective_budget     = min(budget, MAX_TRADE_USD)
            affordable           = int(effective_budget / premium_per_contract) if premium_per_contract > 0 else 0

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
            )
        except Exception as e:
            log.error("_build_selected failed: %s", e)
            return None
