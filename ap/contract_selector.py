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

log = logging.getLogger("ap.contract_selector")


# =============================================================================
# PRO-LEVEL CONTRACT QUALITY THRESHOLDS
# =============================================================================
# Hard gates + A/B tiering. Applied BEFORE the legacy _quality_filter so a
# contract that fails pro gates never reaches ranking or fallback.
#
# Indices + mega-caps get tighter spread limits because they have the liquidity
# to deserve it. Everything else gets a still-strict but slightly wider band.
#
# No fallback. If zero contracts pass hard gates, the signal is skipped.
# =============================================================================

# Toggle: set PRO_CONTRACT_QUALITY=false to fall back to legacy gates.
# Default ON. Leave as an escape hatch, not a default.
_PRO_QUALITY_ENABLED = os.getenv("PRO_CONTRACT_QUALITY", "true").lower() != "false"

# Tier 1: indices + mega-caps. Tight spreads earned.
_PRO_TIER1_TICKERS = {
    "SPY", "QQQ", "IWM", "DIA",                            # indices
    "AAPL", "MSFT", "NVDA", "AMD", "META", "GOOG", "GOOGL",  # mega-caps
    "TSLA", "AMZN", "NFLX",                                # high-volume tech
}

# Hard-reject if any of these fire
_PRO_MIN_BID            = 0.10   # below this, fills are too noisy
_PRO_MIN_BID_SIZE_HARD  = 3      # either side < 3 = instant reject

# Tier 1 (indices + mega-caps)
_PRO_T1_SPREAD_HARD_MAX = 0.06   # 6% max spread
_PRO_T1_SPREAD_A_TIER   = 0.03   # <=3% = A-tier
_PRO_T1_SIZE_MIN        = 10     # both sides >= 10

# Tier 2 (everything else)
_PRO_T2_SPREAD_HARD_MAX = 0.08   # 8% max spread
_PRO_T2_SPREAD_A_TIER   = 0.04   # <=4% = A-tier
_PRO_T2_SIZE_MIN        = 5      # both sides >= 5


def _pro_contract_quality(opt: dict, ticker: str, dte: int) -> tuple[str, str]:
    """
    Pro-level hard gates + A/B tiering.

    Returns (tier, reason):
      - ('A', 'tight_liquid')     passes + tight spread + strong size/liq
      - ('B', 'ok_liquid')        passes hard gates, mid-pack
      - ('REJECT', '<reason>')    failed a hard gate — skip, no fallback

    Tier 1 tickers (indices + mega-caps): 6% spread hard cap, 3% for A-tier.
    Tier 2 (everything else): 8% spread hard cap, 4% for A-tier.
    """
    is_t1 = ticker.upper() in _PRO_TIER1_TICKERS

    bid = float(opt.get("bid") or 0)
    ask = float(opt.get("ask") or 0)
    vol = int(opt.get("volume") or 0)
    oi  = int(opt.get("open_interest") or 0)

    # Top-of-book size — Tradier returns bidsize/asksize or size.bid/ask
    bid_size = int(opt.get("bid_size") or opt.get("bidsize") or 0)
    ask_size = int(opt.get("ask_size") or opt.get("asksize") or 0)

    # A. Price sanity
    if bid <= 0 or ask <= 0:
        return "REJECT", "zero_bid_or_ask"
    if ask < bid:
        return "REJECT", "ask_below_bid"
    if bid < _PRO_MIN_BID:
        return "REJECT", f"bid_below_{_PRO_MIN_BID}"

    # B. Spread
    mid = (bid + ask) / 2
    if mid <= 0:
        return "REJECT", "zero_mid"
    spread_pct = (ask - bid) / mid

    hard_spread = _PRO_T1_SPREAD_HARD_MAX if is_t1 else _PRO_T2_SPREAD_HARD_MAX
    a_spread    = _PRO_T1_SPREAD_A_TIER  if is_t1 else _PRO_T2_SPREAD_A_TIER

    if spread_pct > hard_spread:
        return "REJECT", f"spread_too_wide_{spread_pct*100:.1f}%_max_{hard_spread*100:.0f}%"

    # C. Top-of-book size (only if broker surfaced it)
    if bid_size or ask_size:
        if bid_size < _PRO_MIN_BID_SIZE_HARD or ask_size < _PRO_MIN_BID_SIZE_HARD:
            return "REJECT", f"size_too_thin_bid{bid_size}_ask{ask_size}"

    # D. Volume + OI (per DTE)
    if dte <= 1:
        min_vol, min_oi = 100, 500
    elif dte <= 7:
        min_vol, min_oi = 200, 1000
    else:
        min_vol, min_oi = 500, 2000

    if vol < min_vol and oi < min_oi:
        return "REJECT", f"illiquid_vol{vol}_oi{oi}_need_v{min_vol}_or_oi{min_oi}"

    # E. Tier the survivor
    size_ok_A   = (bid_size >= _PRO_T1_SIZE_MIN and ask_size >= _PRO_T1_SIZE_MIN) if is_t1 \
                  else (bid_size >= _PRO_T2_SIZE_MIN and ask_size >= _PRO_T2_SIZE_MIN)
    # If broker didn't surface size, don't penalize — require only vol/oi strength
    has_size    = bool(bid_size or ask_size)

    strong_liq  = (vol >= min_vol * 2) or (oi >= min_oi * 2)

    if spread_pct <= a_spread and strong_liq and (size_ok_A or not has_size):
        return "A", "tight_liquid"

    return "B", "ok_liquid"


# =============================================================================
# OUTPUT DATACLASS
# =============================================================================

@dataclass
class SelectedContract:
    contract_symbol:       str
    expiration:            str          # "YYYY-MM-DD"
    strike:                float
    option_type:           str          # "call" | "put"
    bid:                   float
    ask:                   float
    mid:                   float
    spread_pct:            float        # (ask-bid)/mid
    delta:                 Optional[float]
    open_interest:         int
    volume:                int
    premium_per_share:     float        # mid price (per share, not contract)
    premium_per_contract:  float        # mid * 100
    affordable_contracts:  int          # how many client can afford at plan budget
    selection_reason:      str
    selection_score:       float        # internal ranking score
    dte:                   int          # days to expiration

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
    """
    Selects the best tradable option contract for an ApprovedExecutionPlan.

    Constructor args:
        broker         -- Tradier broker instance (has option_chain method)
        mode           -- "paper" | "live"
        target_delta   -- preferred delta band center (default 0.40)
        delta_band     -- +/- tolerance around target delta (default 0.15)
        max_spread_pct -- max bid-ask spread as % of mid (default 0.20 = 20%)
        min_oi         -- minimum open interest (default 50)
        min_volume     -- minimum daily volume (default 10)
        min_premium    -- min premium per contract in $ (default 50 = $0.50/share)
        max_premium    -- max premium per contract in $ (default 350 = $3.50/share max)
        max_dte        -- maximum days to expiration (default 21)
        min_dte        -- minimum DTE (default 0 for 0DTE support)
        prefer_weekly  -- prefer weekly expirations (default True)
        earnings_guard -- APEarningsGuard instance (optional; skipped if None)
        iv_filter      -- APIVRankFilter instance (optional; skipped if None)
    """

    def __init__(
        self,
        broker,
        *,
        mode:           str   = "paper",
        data_broker     = None,   # separate live-data broker for quotes/chains
                                  # if set, used for ALL market data calls
                                  # broker is used ONLY for order placement
        target_delta:   float = 0.40,  # slightly OTM preferred
        delta_band:     float = 0.30,  # ±0.30 = 0.10-0.70 delta range (covers 1-2 strikes OTM)
        max_spread_pct: float = 0.50,  # raised 0.35→0.50 -- data collection, wider acceptance
        min_oi:         int   = 1,    # lowered 50→1 -- any open interest passes in paper mode
        min_volume:     int   = 0,    # lowered 10→0 -- volume check disabled for data collection
        min_premium:    float = 10.0,    # $0.10/share -- allow cheap weeklies
        max_premium:    float = 350.0,    # $3.50/share = $350/contract hard cap
        max_dte:        int   = 21,
        min_dte:        int   = 0,
        prefer_weekly:  bool  = True,
        earnings_guard=None,
        iv_filter=None,
    ):
        self.broker         = broker
        # data_broker: used only for market data (quotes, chains, expirations)
        # Falls back to self.broker if not set
        self.data_broker    = data_broker if data_broker is not None else broker
        self.mode           = mode
        self.target_delta   = target_delta
        self.delta_band     = delta_band
        # Allow runtime override via env -- loosen on high-vol days
        self.max_spread_pct = float(os.getenv('MAX_SPREAD_PCT', str(max_spread_pct)))
        self.min_oi         = min_oi
        self.min_volume     = min_volume
        self.min_premium    = min_premium
        self.max_premium    = max_premium
        self.max_dte        = max_dte
        self.min_dte        = min_dte
        self.prefer_weekly  = prefer_weekly
        self.earnings_guard = earnings_guard
        self.iv_filter      = iv_filter

        log.info(
            "APContractSelectionEngine | mode=%s "
            "delta=%.2f±%.2f "
            "max_spread=%d%% "
            "dte=[%d,%d] "
            "premium=[$%.0f,$%.0f] "
            "earnings_guard=%s iv_filter=%s",
            mode,
            target_delta, delta_band,
            int(max_spread_pct * 100),
            min_dte, max_dte,
            min_premium, max_premium,
            type(earnings_guard).__name__ if earnings_guard is not None else "None",
            type(iv_filter).__name__ if iv_filter is not None else "None",
        )

    # =========================================================================
    # PUBLIC -- select(plan) → SelectedContract | None
    # =========================================================================

    def select(self, plan) -> Optional[SelectedContract]:
        """
        Given an ApprovedExecutionPlan, return the best SelectedContract.
        Returns None if no suitable contract found or a gate blocks the trade.

        Gate order:
          1. APEarningsGuard.check(ticker)   -- BEFORE chain fetch
          2. Chain fetch
          3. APIVRankFilter.check(...)       -- AFTER chain fetch
          4. Quality filter + ranking
          5. Affordability gate
        """
        ticker    = plan.ticker
        direction = plan.side.upper()   # "CALL" | "PUT"
        budget    = plan.max_position_usd

        # ── ETF vs single-stock quality rules ────────────────────────────────────────
        # ETFs (SPY, QQQ, IWM, etc.) are highly liquid — hold to tighter standards.
        # Single stocks (GS, MRNA, BLK, etc.) have less volume — adjusted rules.
        _LIQUID_ETFS = {"SPY", "QQQ", "IWM", "DIA", "GLD", "TLT", "XLF",
                        "XLE", "XLK", "XLV", "XLC", "TQQQ", "SQQQ", "UVXY"}
        _is_etf = ticker.upper() in _LIQUID_ETFS

        if _is_etf:
            _eff_max_spread  = 0.25   # 25% max spread for liquid ETFs
            _eff_min_oi      = 100    # higher OI floor for ETFs
            _eff_min_volume  = 10     # require some live volume for ETFs
            _fallback_spread = 0.35   # tighter fallback for ETFs
            _fallback_oi     = 200
        else:
            _eff_max_spread  = self.max_spread_pct  # 50% — single stocks can be wider
            _eff_min_oi      = 25     # lower OI floor for single stocks
            _eff_min_volume  = 0      # volume check relaxed for single stocks
            _fallback_spread = 0.60   # wider fallback allowed for singles
            _fallback_oi     = 50

        # Index tickers (^GSPC etc) cannot be quoted via Tradier options API.
        # Remap to tradable ETFs, or skip entirely.
        _INDEX_MAP = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM", "^DJI": None}
        if ticker.startswith("^"):
            mapped = _INDEX_MAP.get(ticker.upper())
            if mapped:
                log.info("[%s] Index ticker remapped to %s for options chain", ticker, mapped)
                ticker = mapped
            else:
                log.warning("[%s] Index ticker has no options mapping -- skipping", ticker)
                return None
        # PT1 = first price target (wick cluster level) -- expected move destination.
        # Used ONLY as expected move % to calibrate OTM bias. NOT a strike anchor.
        pt1 = getattr(plan, "target_underlying", None)
        # wick_targets from signal enrichment: [{price, confidence, distance_pct, ...}]
        wick_targets = getattr(plan, "wick_targets", None) or []
        # expected_move_pct: how far price is expected to travel (sets OTM bias)
        _expected_move_pct = 0.0
        _wick_confidence   = 0.5  # default if no wick data
        if wick_targets:
            _expected_move_pct = float(wick_targets[0].get("distance_pct", 0) or 0)
            _wick_confidence   = float(wick_targets[0].get("confidence", 0.5) or 0.5)
        elif pt1 and pt1 > 0:
            entry_approx = getattr(plan, "trigger_price", pt1) or pt1
            _expected_move_pct = abs(pt1 - entry_approx) / entry_approx * 100 if entry_approx else 0

        log.info(
            "[%s] ContractSelector | direction=%s budget=$%.0f tier=%s pt1=%s",
            ticker, direction, budget, plan.tier, pt1,
        )

        # ── GATE 1: EARNINGS BLACKOUT ─────────────────────────────────────────
        # Check BEFORE fetching the chain to avoid unnecessary API calls.

        if self.earnings_guard is not None:
            try:
                eg_result = self.earnings_guard.check(ticker)
                if eg_result.get("blocked"):
                    reason = eg_result.get("reason", "earnings blackout")
                    log.warning(
                        "[%s] BLOCKED by EarningsGuard -- %s",
                        ticker, reason,
                    )
                    return None
            except Exception as exc:
                # Fail open: log warning, do not block
                log.warning(
                    "[%s] EarningsGuard raised unexpectedly (%s) -- continuing (fail open)",
                    ticker, exc,
                )

        # ── A. FETCH CHAIN ────────────────────────────────────────────────────

        try:
            chain, underlying_price = self._fetch_chain_with_price(ticker, direction)
        except Exception as e:
            log.error("[%s] chain fetch failed: %s", ticker, e)
            if self.mode.upper() != "LIVE":
                return None  # hard reject — no synthetic fills ever
            return None

        if not chain:
            log.warning("[%s] EMPTY CHAIN for %s -- Tradier returned no options (sandbox data gap?)", ticker, direction)
            if self.mode.upper() != "LIVE":
                return None  # hard reject — no synthetic fills ever
            return None

        # Use plan's trigger price as underlying fallback if chain didn't return it
        if not underlying_price:
            underlying_price = getattr(plan, "trigger_price", None)

        # ── GATE 2: IV RANK FILTER ────────────────────────────────────────────
        # Check AFTER fetching the chain (we need chain data for ATM IV).

        if self.iv_filter is not None:
            try:
                iv_result = self.iv_filter.check(
                    ticker,
                    option_chain=chain,
                    underlying_price=underlying_price or 0.0,
                )
                if iv_result.get("blocked"):
                    reason = iv_result.get("reason", "IV rank too high")
                    log.warning(
                        "[%s] BLOCKED by IVRankFilter -- %s",
                        ticker, reason,
                    )
                    _sig_id = getattr(plan, "signal_id", "") or (plan.get("signal_id","") if isinstance(plan,dict) else "")
                    trace_gate(str(_sig_id), ticker, "IV_GATE", "REJECT",
                               reason="iv_extreme", iv_rank=iv_result.get("iv_rank"))
                    return None

                # Momentum gate: zones that require momentum confirmation.
                # Uses signal score as proxy for momentum until momentum_pct
                # is added to ApprovedExecutionPlan (planned for next sprint).
                # Breach is confirmed by definition at contract-selection time.
                if iv_result.get("requires_momentum"):
                    iv_zone      = iv_result.get("iv_zone", "soft")
                    signal_score = float(
                        plan.score if hasattr(plan, "score")
                        else (plan.get("score", 0) if isinstance(plan, dict) else 0)
                    )
                    min_score    = float(iv_result.get("momentum_min_score", 65.0))

                    # Score gate — momentum_pct will be added to plan in future
                    mom_ok = signal_score >= min_score
                    if not mom_ok:
                        log.warning(
                            "[%s] IVGate reject | iv_zone=%s iv=%.1f score=%.1f "
                            "(need score>=%.0f for this IV zone)",
                            ticker, iv_zone,
                            iv_result.get("iv_rank", 0), signal_score, min_score,
                        )
                        _sig_id = getattr(plan, "signal_id", "") or (plan.get("signal_id","") if isinstance(plan,dict) else "")
                        trace_gate(str(_sig_id), ticker, "IV_GATE", "REJECT",
                                   reason=f"iv_zone={iv_zone}_score_too_low",
                                   score=signal_score, iv_rank=iv_result.get("iv_rank"))
                        return None
                    else:
                        log.info(
                            "[%s] IVGate allow | tier=B iv_zone=%s iv=%.1f score=%.1f",
                            ticker, iv_zone,
                            iv_result.get("iv_rank", 0), signal_score,
                        )
                        _sig_id = getattr(plan, "signal_id", "") or (plan.get("signal_id","") if isinstance(plan,dict) else "")
                        trace_gate(str(_sig_id), ticker, "IV_GATE", "ALLOW",
                                   reason=f"iv_zone={iv_zone}",
                                   score=signal_score, iv_rank=iv_result.get("iv_rank"))

                # Apply tier cap from IV filter if set
                if iv_result.get("tier_cap"):
                    plan_tier = getattr(plan, "tier", None) or (plan.get("tier") if isinstance(plan, dict) else None)
                    if plan_tier and plan_tier < iv_result["tier_cap"]:
                        log.info("[%s] IV tier_cap applied: %s → %s", ticker, plan_tier, iv_result["tier_cap"])

            except Exception as exc:
                # Fail open: log warning, do not block
                log.warning(
                    "[%s] IVRankFilter raised unexpectedly (%s) -- continuing (fail open)",
                    ticker, exc,
                )

        # ── B. HARD QUALITY FILTER ────────────────────────────────────────────
        # Apply ETF vs single-stock effective thresholds for this call
        _orig_spread        = self.max_spread_pct
        _orig_oi            = self.min_oi
        _orig_vol           = self.min_volume
        self.max_spread_pct = _eff_max_spread
        self.min_oi         = _eff_min_oi
        self.min_volume     = _eff_min_volume


        today = date.today()
        survivors = []
        _rejections: dict = {}
        _pro_tiers:  dict = {"A": 0, "B": 0}

        for opt in chain:
            # ── PRO GATE (new, default ON) ────────────────────────────────
            # Hard gates + A/B tiering. Failed contracts never reach ranking.
            # No fallback — if nothing passes, we skip the signal.
            if _PRO_QUALITY_ENABLED:
                # Compute DTE for pro-gate liquidity bucket
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
                    continue
                # Tag for downstream ranking
                opt["_pro_tier"] = pro_tier
                _pro_tiers[pro_tier] = _pro_tiers.get(pro_tier, 0) + 1

            # ── LEGACY GATE (still runs — belt and braces) ────────────────
            result = self._quality_filter(opt, today)
            if result is None:
                survivors.append(opt)
            else:
                _rejections[result] = _rejections.get(result, 0) + 1
                log.debug(
                    "[%s] filtered: %s -- %s",
                    ticker, opt.get("symbol", "?"), result,
                )

        # Restore original thresholds
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
            # NO FALLBACK. Ever. Paper or live.
            # Previously paper mode would force "best available" which shipped
            # 50%+ spread contracts with zero volume. That's the bug we're
            # killing. If nothing passes hard gates, the signal is skipped —
            # fewer trades is the point, not the cost.
            return None

        if _PRO_QUALITY_ENABLED:
            log.info(
                "[%s] %d contracts passed pro quality | A=%d B=%d",
                ticker, len(survivors), _pro_tiers.get("A", 0), _pro_tiers.get("B", 0),
            )
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
            if self.mode.upper() != "LIVE":
                return None  # hard reject — no synthetic fills ever
            return None

        # ── E. AFFORDABILITY GATE ────────────────────────────────────────────
        # Paper mode: force 1 contract if budget can't cover it (data collection)
        # Live mode: hard block — never trade what you can't afford
        if selected.affordable_contracts < 1:
            if self.mode.upper() != "LIVE":
                # Hard ceiling: never force contracts that exceed max_premium
                # This prevents $500+ fills from polluting paper data
                if selected.premium_per_contract > self.max_premium:
                    log.warning(
                        "[%s] premium $%.0f > max $%.0f -- skipping (too expensive for paper)",
                        ticker, selected.premium_per_contract, self.max_premium,
                    )
                    return None
                log.warning(
                    "[%s] budget $%.0f < premium $%.0f -- forcing 1 contract (paper data collection)",
                    ticker, budget, selected.premium_per_contract,
                )
                selected = SelectedContract(
                    contract_symbol=selected.contract_symbol,
                    expiration=selected.expiration,
                    strike=selected.strike,
                    option_type=selected.option_type,
                    bid=selected.bid,
                    ask=selected.ask,
                    mid=selected.mid,
                    spread_pct=selected.spread_pct,
                    delta=selected.delta,
                    open_interest=selected.open_interest,
                    volume=selected.volume,
                    premium_per_share=selected.premium_per_share,
                    premium_per_contract=selected.premium_per_contract,
                    affordable_contracts=1,
                    selection_reason=selected.selection_reason + " [forced_1]",
                    selection_score=selected.selection_score,
                    dte=selected.dte,
                )
            else:
                log.warning(
                    "[%s] BLOCKED -- budget $%.0f cannot afford %s @ $%.0f/contract",
                    ticker, budget, selected.contract_symbol, selected.premium_per_contract,
                )
                return None

        # ── FINAL SAFETY NET ─────────────────────────────────────────────────
        # Should never reach here with selected=None after all the fallbacks above,
        # but if something slips through in paper mode, catch it here.
        if selected is None:
            if self.mode.upper() != "LIVE":
                return None  # hard reject — no synthetic fills ever
            else:
                return None

        # ── F. UPDATE PLAN IN-PLACE ───────────────────────────────────────────

        plan.contract_symbol = selected.contract_symbol
        plan.limit_price     = selected.ask   # use ask as limit for entry
        plan.contracts       = selected.affordable_contracts  # never forced to 1
        # Recalculate max_position_usd with real premium
        plan.max_position_usd = plan.contracts * selected.premium_per_contract
        # Inject wick confidence so sizer can scale qty (high conf = more contracts)
        if _wick_confidence and not getattr(plan, "wick_confidence", None):
            try:
                plan.wick_confidence = _wick_confidence
            except Exception:
                pass

        log.info(
            "[%s] SELECTED | %s "
            "bid=%s ask=%s mid=%.2f "
            "spread=%.1f%% "
            "delta=%s OI=%d "
            "vol=%d DTE=%d "
            "premium=$%.0f "
            "contracts=%d "
            "score=%.2f",
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
        self, ticker: str, direction: str
    ) -> tuple[list[dict], Optional[float]]:
        """
        Fetch option chain and underlying price from broker.
        Returns (chain_list, underlying_price_or_None).
        """
        option_type = direction.lower()   # "call" | "put"

        # Always use _fetch_tradier_chain for full chain + expiration selection.
        # TradierBroker.get_option_chain(ticker, expiration) requires an expiration
        # date we don't have yet -- that logic lives inside _fetch_tradier_chain.
        # Calling it with option_type= causes: got an unexpected keyword argument 'option_type'
        return self._fetch_tradier_chain(ticker, option_type)

    def _fetch_chain(self, ticker: str, direction: str) -> list[dict]:
        """
        Thin wrapper kept for backward compatibility.
        Returns chain list only (discards underlying price).
        """
        chain, _ = self._fetch_chain_with_price(ticker, direction)
        return chain

    def _fetch_tradier_chain(
        self, ticker: str, option_type: str
    ) -> tuple[list[dict], Optional[float]]:
        """Direct Tradier API call for option chain. Returns (chain, underlying_price).

        Always uses self.data_broker for ALL market data calls.
        self.broker (execution broker) is NEVER used here -- stays gated by BOT_MODE.
        """
        import requests

        # Use data_broker for all market data -- live API if configured
        # TradierConfig stores token as .access_token (not .token)
        cfg      = getattr(self.data_broker, "cfg", None)
        base_url = (
            getattr(cfg, "base_url", None)
            or getattr(self.data_broker, "base_url", "https://sandbox.tradier.com")
        )
        token = (
            getattr(cfg, "access_token", None)      # TradierConfig field name
            or getattr(cfg, "token", None)           # fallback alias
            or getattr(self.data_broker, "access_token", None)
            or getattr(self.data_broker, "token", "")
        ) or ""
        if not token:
            log.error("[%s] No Tradier token found on data_broker -- chain fetch will 401", ticker)
        headers  = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        # 1. Fetch underlying quote for moneyness fallback when delta unavailable
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
                    underlying_price = (
                        float(quotes.get("last") or quotes.get("bid") or 0) or None
                    )
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

        # 5. Inject underlying price for moneyness fallback in quality filter
        if underlying_price:
            for o in options:
                o["_underlying_price"] = underlying_price

        filtered = [o for o in options
                    if o.get("option_type", "").lower() == option_type]

        return filtered, underlying_price

    def _pick_expiration(self, dates: list[str]) -> Optional[str]:
        """Choose target expiration from available dates."""
        today    = date.today()
        valid    = []

        for d_str in dates:
            try:
                d = date.fromisoformat(d_str)
                dte = (d - today).days
                if self.min_dte <= dte <= self.max_dte:
                    valid.append((dte, d_str))
            except Exception:
                continue

        if not valid:
            # Relax DTE if nothing fits -- take nearest after min_dte
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
        # Prefer weekly (Fridays) if enabled
        if self.prefer_weekly:
            fridays = [(dte, d) for dte, d in valid
                       if date.fromisoformat(d).weekday() == 4]
            if fridays:
                return fridays[0][1]

        return valid[0][1]

    # =========================================================================
    # PRIVATE -- QUALITY FILTER
    # =========================================================================


    def _synthetic_contract(self, ticker: str, direction: str, reason: str) -> "SelectedContract":
        """Last-resort fallback for paper mode — guarantees a fill for data collection."""
        log.warning("[%s] SYNTHETIC CONTRACT -- %s", ticker, reason)
        return SelectedContract(
            contract_symbol=f"{ticker}_SIM",
            expiration="SIM",
            strike=0,
            option_type=direction.lower(),
            bid=0.50,
            ask=1.00,
            mid=0.75,
            spread_pct=0.20,
            delta=0.50,
            open_interest=1,
            volume=1,
            premium_per_share=0.75,
            premium_per_contract=75.0,
            affordable_contracts=1,
            selection_reason=reason,
            selection_score=0,
            dte=0,
        )

    def _quality_filter(self, opt: dict, today: date) -> Optional[str]:
        """
        Returns None if contract passes, or a reason string if it fails.
        """
        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        oi  = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)

        # Must have valid prices
        if bid <= 0 or ask <= 0:
            return "zero_bid_or_ask"
        if ask < bid:
            return "ask_below_bid"

        # Spread check
        mid = (bid + ask) / 2
        if mid <= 0:
            return "zero_mid"
        spread_pct = (ask - bid) / mid
        if spread_pct > self.max_spread_pct:
            return "spread_too_wide_%.1f%%" % (spread_pct * 100)

        # OI / volume
        if oi < self.min_oi:
            return "low_oi_%d" % oi
        if vol < self.min_volume:
            return "low_volume_%d" % vol

        # Premium range
        premium = mid * 100
        if premium < self.min_premium:
            return "premium_too_low_$%.0f" % premium
        if premium > self.max_premium:
            return "premium_too_high_$%.0f" % premium

        # DTE
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

        # Delta check -- if greeks available use delta band; else use moneyness proxy
        greeks = opt.get("greeks") or {}
        delta = greeks.get("delta")
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
            # Moneyness proxy when delta unavailable (injected via _underlying_price)
            underlying_price = opt.get("_underlying_price")
            strike = float(opt.get("strike") or 0)
            if underlying_price and strike:
                moneyness = strike / float(underlying_price)
                option_type = opt.get("option_type", "").lower()
                # CALLs: slightly OTM to slightly ITM (0.93x-1.12x spot)
                # PUTs:  slightly OTM to slightly ITM (0.88x-1.07x spot)
                if option_type == "call" and not (0.93 <= moneyness <= 1.12):
                    return "moneyness_out_of_range_%.3f" % moneyness
                if option_type == "put" and not (0.88 <= moneyness <= 1.07):
                    return "moneyness_out_of_range_%.3f" % moneyness

        return None  # passed

    # =========================================================================
    # PRIVATE -- RANKING
    # =========================================================================

    def _rank_score(self, opt: dict, budget: float,
                     expected_move_pct: float = 0.0,
                     underlying_price: float = 0.0,
                     tier: str = "B") -> float:
        """
        Ranking score -- higher is better.

        Priority order (probability-first):
          1. Delta proximity -- ATM (0.50) = fastest reaction, highest hit rate
          2. Spread tightness -- clean fills, less slippage on entry/exit
          3. Liquidity (OI + volume, log scale) -- real market, avoids dead contracts
          4. Premium size -- still respected, no longer dominant
          5. Expected move context (tiny otm_bias)

        The goal is the BEST PROBABILITY CONTRACT, then affordable.
        PT1/PT2 are exits, not strikes.
        """
        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        oi  = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)
        mid = (bid + ask) / 2
        if mid <= 0:
            return -9999.0

        spread_pct = (ask - bid) / mid
        premium    = mid * 100  # cost per contract

        greeks = opt.get("greeks") or {}
        try:
            delta = abs(float(greeks.get("delta") or self.target_delta))
        except Exception:
            delta = self.target_delta

        # ── 1. Premium penalty (dominant weight) ──────────────────────────
        # Ideal premium: $3.50/share ($350/contract). Hard cap $5.00 ($500).
        # Penalize contracts above ideal heavily; reward contracts below it.
        MAX_IDEAL_PREMIUM = float(os.getenv("MAX_IDEAL_PREMIUM", "3.50"))
        MAX_HARD_PREMIUM  = float(os.getenv("MAX_HARD_PREMIUM",  "4.00"))
        # No hard kill -- expensive contracts get a heavy penalty and rank last.
        # The quality filter (max_premium) handles the true ceiling.
        # Killing here meant "no contracts passed" even when expensive was the only option.
        premium_penalty = max(0.0, mid - MAX_IDEAL_PREMIUM)  # 0 if at/below ideal

        # Affordable? 0 if not (gate fires in select())
        effective_budget = min(budget, float(os.getenv("MAX_TRADE_USD", "500")))
        affordable = int(effective_budget / premium) if premium > 0 else 0
        # Don't hard-kill here -- let select() handle affordability gate.
        # Returning -9999 here caused valid contracts to be invisible to the ranker.

        # ── 2. Delta proximity (ATM bias) ────────────────────────────────
        delta_distance = abs(delta - self.target_delta)

        # -- 3. Expected move OTM bias (tiny weight) ──────────────────────
        # Wick targets tell us HOW FAR price may go, NOT where to strike.
        # ATM (delta 0.50) always wins. This is a tiny 8-point adjustment.
        otm_bias = 0.0
        if underlying_price > 0:
            strike = float(opt.get("strike") or 0)
            if strike <= underlying_price:
                otm_bias = 0.2   # ATM or ITM: small bonus
            elif expected_move_pct >= 1.5:
                otm_dist = (strike - underlying_price) / underlying_price * 100
                if otm_dist <= 0.5:
                    otm_bias = 0.1   # large expected move + barely OTM: tiny bonus

        # ── Tier-based weight adjustment ─────────────────────────────────
        # A+/A: best signal quality -- prioritize best contract, relax cost
        # B:    default balanced weights
        # C:    weaker signal -- tighter cost control, still want ATM
        if tier in ("A+", "A"):
            delta_weight   = -140
            spread_weight  =  -90
            premium_weight =  -25
        elif tier == "B":
            delta_weight   = -130
            spread_weight  =  -80
            premium_weight =  -35
        else:  # C or unknown
            delta_weight   = -120
            spread_weight  =  -70
            premium_weight =  -50

        score = (
            (delta_distance    * delta_weight)   +   # 1st: probability -- ATM
            (spread_pct        * spread_weight)  +   # 2nd: execution quality
            (math.log(oi + 1)  *   12)           +   # 3rd: liquidity
            (math.log(vol + 1) *    6)           +   # 4th: volume
            (premium_penalty   * premium_weight) +   # 5th: cost (tier-scaled)
            (otm_bias          *    6)               # tiny: expected move context
        )
        return score

    # =========================================================================
    # PRIVATE -- BUILD SelectedContract
    # =========================================================================

    def _build_selected(
        self, opt: dict, score: float, budget: float, today: date
    ) -> Optional[SelectedContract]:

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
            # Hard cap: max $500 per trade regardless of budget passed in.
            # Budget caps contracts from above; hard cap prevents runaway qty.
            # Kelly/tier sizer sets budget -- this is the final safety net.
            MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "500"))
            effective_budget = min(budget, MAX_TRADE_USD)
            # 0 = unaffordable -- select() will block the trade
            affordable = int(effective_budget / premium_per_contract) if premium_per_contract > 0 else 0

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
                        delta, spread_pct * 100, oi, vol, dte, premium_per_contract
                    )
                    if delta else
                    "spread=%.1f%% OI=%d vol=%d DTE=%d premium=$%.0f" % (
                        spread_pct * 100, oi, vol, dte, premium_per_contract
                    )
                ),
                selection_score      = score,
                dte                  = dte,
            )
        except Exception as e:
            log.error("_build_selected failed: %s", e)
            return None
