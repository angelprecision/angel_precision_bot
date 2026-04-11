# ap/contract_selector.py — APContractSelectionEngine
# =============================================================================
# Unified contract selection. Takes an ApprovedExecutionPlan, returns the
# single best tradable contract + real sizing based on actual premium.
#
# Selection algorithm:
#   A. Choose expiration  (0DTE / weekly / nearest)
#   B. Filter to direction (CALL / PUT)
#   C. Hard quality filters (spread, OI, volume, DTE, delta)
#   D. Rank survivors (delta fit, spread, OI, volume, premium fit)
#   E. Price sanity (buy-side: prefer ask when spread is tight)
#   F. Affordability → final contract count from real premium
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
        broker         — Tradier broker instance (has option_chain method)
        mode           — "paper" | "live"
        target_delta   — preferred delta band center (default 0.40)
        delta_band     — +/- tolerance around target delta (default 0.15)
        max_spread_pct — max bid-ask spread as % of mid (default 0.20 = 20%)
        min_oi         — minimum open interest (default 50)
        min_volume     — minimum daily volume (default 10)
        min_premium    — min premium per contract in $ (default 50 = $0.50/share)
        max_premium    — max premium per contract in $ (default 2000 = $20/share)
        max_dte        — maximum days to expiration (default 21)
        min_dte        — minimum DTE (default 0 for 0DTE support)
        prefer_weekly  — prefer weekly expirations (default True)
    """

    def __init__(
        self,
        broker,
        *,
        mode:           str   = "paper",
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
    ):
        self.broker         = broker
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

        log.info(
            f"APContractSelectionEngine | mode={mode} "
            f"delta={target_delta}±{delta_band} "
            f"max_spread={max_spread_pct*100:.0f}% "
            f"dte=[{min_dte},{max_dte}] "
            f"premium=[${min_premium:.0f},${max_premium:.0f}]"
        )

    # =========================================================================
    # PUBLIC — select(plan) → SelectedContract | None
    # =========================================================================

    def select(self, plan) -> Optional[SelectedContract]:
        """
        Given an ApprovedExecutionPlan, return the best SelectedContract.
        Returns None if no suitable contract found.
        
        Also updates plan.contract_symbol, plan.limit_price, and 
        plan.contracts to reflect real-premium sizing.
        """
        ticker    = plan.ticker
        direction = plan.side.upper()   # "CALL" | "PUT"
        budget    = plan.max_position_usd

        log.info(
            f"[{ticker}] ContractSelector | direction={direction} "
            f"budget=${budget:.0f} tier={plan.tier}"
        )

        # ── A. FETCH CHAIN ────────────────────────────────────────────────────

        try:
            chain = self._fetch_chain(ticker, direction)
        except Exception as e:
            log.error(f"[{ticker}] chain fetch failed: {e}")
            return None

        if not chain:
            log.warning(f"[{ticker}] empty chain for {direction}")
            return None

        # ── B. HARD QUALITY FILTER ────────────────────────────────────────────

        today = date.today()
        survivors = []
        for opt in chain:
            result = self._quality_filter(opt, today)
            if result is None:
                survivors.append(opt)
            else:
                log.debug(f"[{ticker}] filtered: {opt.get('symbol','?')} — {result}")

        if not survivors:
            log.warning(f"[{ticker}] no contracts passed quality filter")
            return None

        log.info(f"[{ticker}] {len(survivors)} contracts passed quality filter")

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

        # ── E. AFFORDABILITY GATE — LIVE SAFETY ─────────────────────────────
        # If budget cannot cover even 1 contract, block. Never force to 1.
        if selected.affordable_contracts < 1:
            log.warning(
                f"[{ticker}] BLOCKED — budget ${budget:.0f} cannot afford "
                f"{selected.contract_symbol} "
                f"@ ${selected.premium_per_contract:.0f}/contract"
            )
            return None

        # ── F. UPDATE PLAN IN-PLACE ───────────────────────────────────────────

        plan.contract_symbol = selected.contract_symbol
        plan.limit_price     = selected.ask   # use ask as limit for entry
        plan.contracts       = selected.affordable_contracts  # never forced to 1
        # Recalculate max_position_usd with real premium
        plan.max_position_usd = plan.contracts * selected.premium_per_contract

        log.info(
            f"[{ticker}] ✅ SELECTED | {selected.contract_symbol} "
            f"bid={selected.bid} ask={selected.ask} mid={selected.mid:.2f} "
            f"spread={selected.spread_pct*100:.1f}% "
            f"delta={selected.delta} OI={selected.open_interest} "
            f"vol={selected.volume} DTE={selected.dte} "
            f"premium=${selected.premium_per_contract:.0f} "
            f"contracts={selected.affordable_contracts} "
            f"score={selected.selection_score:.2f}"
        )
        return selected

    # =========================================================================
    # PRIVATE — CHAIN FETCH
    # =========================================================================

    def _fetch_chain(self, ticker: str, direction: str) -> list[dict]:
        """
        Fetch option chain from broker.
        Supports Tradier broker interface.
        Returns list of option dicts with keys:
          symbol, strike, expiration_date, bid, ask, last,
          open_interest, volume, greeks (delta etc.)
        """
        option_type = direction.lower()   # "call" | "put"

        # Try broker's option chain method
        if hasattr(self.broker, "get_option_chain"):
            chain = self.broker.get_option_chain(ticker, option_type=option_type)
            return chain or []

        if hasattr(self.broker, "option_chain"):
            chain = self.broker.option_chain(ticker, option_type=option_type)
            return chain or []

        # Tradier REST fallback via requests
        return self._fetch_tradier_chain(ticker, option_type)

    def _fetch_tradier_chain(self, ticker: str, option_type: str) -> list[dict]:
        """Direct Tradier API call for option chain."""
        import requests

        cfg      = getattr(self.broker, "cfg", None)
        base_url = (getattr(cfg, "base_url", None) or
                    getattr(self.broker, "base_url", "https://sandbox.tradier.com"))
        token    = (getattr(cfg, "token", None) or
                    getattr(self.broker, "token", ""))
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
            return []

        # 3. Pick best expiration
        target_exp = self._pick_expiration(dates)
        if not target_exp:
            return []

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

        return [o for o in options
                if o.get("option_type", "").lower() == option_type]

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
            # Relax DTE if nothing fits — take nearest after min_dte
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
    # PRIVATE — QUALITY FILTER
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
            return f"spread_too_wide_{spread_pct*100:.1f}%"

        # OI / volume
        if oi < self.min_oi:
            return f"low_oi_{oi}"
        if vol < self.min_volume:
            return f"low_volume_{vol}"

        # Premium range
        premium = mid * 100
        if premium < self.min_premium:
            return f"premium_too_low_${premium:.0f}"
        if premium > self.max_premium:
            return f"premium_too_high_${premium:.0f}"

        # DTE
        exp_str = opt.get("expiration_date", "")
        if exp_str:
            try:
                exp = date.fromisoformat(exp_str)
                dte = (exp - today).days
                if dte < self.min_dte:
                    return f"dte_too_low_{dte}"
                if dte > self.max_dte:
                    return f"dte_too_high_{dte}"
            except Exception:
                return "invalid_expiration"

        # Delta check — if greeks available use delta band; else use moneyness proxy
        greeks = opt.get("greeks") or {}
        delta = greeks.get("delta")
        if delta is not None:
            try:
                delta = abs(float(delta))
                min_d = max(0.05, self.target_delta - self.delta_band)
                max_d = min(0.95, self.target_delta + self.delta_band)
                if delta < min_d or delta > max_d:
                    return f"delta_out_of_band_{delta:.2f}"
            except Exception:
                pass
        else:
            # Moneyness proxy when delta unavailable (injected via _underlying_price)
            underlying_price = opt.get("_underlying_price")
            strike = float(opt.get("strike") or 0)
            if underlying_price and strike:
                moneyness = strike / float(underlying_price)
                option_type = opt.get("option_type", "").lower()
                # CALLs: slightly OTM to slightly ITM (0.93x–1.12x spot)
                # PUTs:  slightly OTM to slightly ITM (0.88x–1.07x spot)
                if option_type == "call" and not (0.93 <= moneyness <= 1.12):
                    return f"moneyness_out_of_range_{moneyness:.3f}"
                if option_type == "put" and not (0.88 <= moneyness <= 1.07):
                    return f"moneyness_out_of_range_{moneyness:.3f}"

        return None  # passed

    # =========================================================================
    # PRIVATE — RANKING
    # =========================================================================

    def _rank_score(self, opt: dict, budget: float) -> float:
        """
        Ranking score — higher is better.
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

        # Premium fit — score 0-1 based on how many contracts fit in budget
        # 0 = unaffordable — do NOT force to 1 here, gate in select()
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
    # PRIVATE — BUILD SelectedContract
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
            # 0 = unaffordable — select() will block the trade
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
                    f"delta={delta:.2f} spread={spread_pct*100:.1f}% "
                    f"OI={oi} vol={vol} DTE={dte} "
                    f"premium=${premium_per_contract:.0f}"
                    if delta else
                    f"spread={spread_pct*100:.1f}% OI={oi} vol={vol} "
                    f"DTE={dte} premium=${premium_per_contract:.0f}"
                ),
                selection_score      = score,
                dte                  = dte,
            )
        except Exception as e:
            log.error(f"_build_selected failed: {e}")
            return None
