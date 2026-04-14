# ap/contract_selector.py -- APContractSelectionEngine
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
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

log = logging.getLogger("ap.contract_selector")


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
        max_premium    -- max premium per contract in $ (default 2000 = $20/share)
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
        target_delta:   float = 0.40,
        delta_band:     float = 0.15,
        max_spread_pct: float = 0.20,
        min_oi:         int   = 50,
        min_volume:     int   = 10,
        min_premium:    float = 50.0,
        max_premium:    float = 2000.0,
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
        self.max_spread_pct = max_spread_pct
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

        log.info(
            "[%s] ContractSelector | direction=%s budget=$%.0f tier=%s",
            ticker, direction, budget, plan.tier,
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
            return None

        if not chain:
            log.warning("[%s] empty chain for %s", ticker, direction)
            return None

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
                    return None
            except Exception as exc:
                # Fail open: log warning, do not block
                log.warning(
                    "[%s] IVRankFilter raised unexpectedly (%s) -- continuing (fail open)",
                    ticker, exc,
                )

        # ── B. HARD QUALITY FILTER ────────────────────────────────────────────

        today = date.today()
        survivors = []
        for opt in chain:
            result = self._quality_filter(opt, today)
            if result is None:
                survivors.append(opt)
            else:
                log.debug(
                    "[%s] filtered: %s -- %s",
                    ticker, opt.get("symbol", "?"), result,
                )

        if not survivors:
            log.warning("[%s] no contracts passed quality filter", ticker)
            return None

        log.info("[%s] %d contracts passed quality filter", ticker, len(survivors))

        # ── C. RANK ───────────────────────────────────────────────────────────

        scored = []
        for opt in survivors:
            s = self._rank_score(opt, budget)
            scored.append((s, opt))

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best = scored[0]

        # ── D. BUILD SELECTED CONTRACT ────────────────────────────────────────

        selected = self._build_selected(best, best_score, budget, today)
        if selected is None:
            return None

        # ── E. AFFORDABILITY GATE -- LIVE SAFETY ─────────────────────────────
        # If budget cannot cover even 1 contract, block. Never force to 1.
        if selected.affordable_contracts < 1:
            log.warning(
                "[%s] BLOCKED -- budget $%.0f cannot afford %s "
                "@ $%.0f/contract",
                ticker, budget,
                selected.contract_symbol,
                selected.premium_per_contract,
            )
            return None

        # ── F. UPDATE PLAN IN-PLACE ───────────────────────────────────────────

        plan.contract_symbol = selected.contract_symbol
        plan.limit_price     = selected.ask   # use ask as limit for entry
        plan.contracts       = selected.affordable_contracts  # never forced to 1
        # Recalculate max_position_usd with real premium
        plan.max_position_usd = plan.contracts * selected.premium_per_contract

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
        cfg      = getattr(self.data_broker, "cfg", None)
        base_url = (getattr(cfg, "base_url", None) or
                    getattr(self.data_broker, "base_url", "https://sandbox.tradier.com"))
        token    = (getattr(cfg, "token", None) or
                    getattr(self.data_broker, "token", ""))
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

    def _rank_score(self, opt: dict, budget: float) -> float:
        """
        Ranking score -- higher is better.
        Weights:
          delta fit     -40   (distance from target delta)
          spread        -25   (tighter is better)
          OI            +15   (log scale)
          volume        +10   (log scale)
          premium fit   +10   (how well premium fits the budget)
        """
        bid = float(opt.get("bid") or 0)
        ask = float(opt.get("ask") or 0)
        oi  = int(opt.get("open_interest") or 0)
        vol = int(opt.get("volume") or 0)
        mid = (bid + ask) / 2

        spread_pct = (ask - bid) / mid if mid > 0 else 1.0

        greeks = opt.get("greeks") or {}
        try:
            delta = abs(float(greeks.get("delta") or self.target_delta))
        except Exception:
            delta = self.target_delta

        delta_distance = abs(delta - self.target_delta)
        premium = mid * 100

        # Premium fit -- score 0-1 based on how many contracts fit in budget
        # 0 = unaffordable -- do NOT force to 1 here, gate in select()
        affordable = int(budget / premium) if premium > 0 else 0
        premium_fit = min(1.0, affordable / 5.0)  # normalize against 5 contracts

        score = (
            (delta_distance * -40) +
            (spread_pct     * -25) +
            (math.log(oi  + 1) * 15) +
            (math.log(vol + 1) * 10) +
            (premium_fit       * 10)
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
            # 0 = unaffordable -- select() will block the trade
            affordable = int(budget / premium_per_contract) if premium_per_contract > 0 else 0

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
