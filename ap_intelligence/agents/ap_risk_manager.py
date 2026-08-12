"""
ap_risk_manager.py — Angel Precision Risk Manager v2 (ELITE)
=============================================================
Adapted from virattt/ai-hedge-fund (MIT License)
Rewritten for Angel Precision's options architecture.

KEY CHANGES from v1:
1. TRUE STOP-BASED SIZING — size from risk dollars + ATR stop + delta, not premium cap
2. NO CONFIDENCE SCALING HERE — confidence lives in portfolio manager only
3. EXPOSURE BUCKET TRACKING — sector/theme/direction, not just ticker correlation
4. CONTRACT QUALITY FILTERS — spread%, OI, volume, delta range, DTE
5. Clean correlation cache using dict of aligned pd.Series

Inputs from your scanner:
  - underlying_price
  - atr_value (ATR of underlying — from ap_scanner_utils.atr_stop)
  - option_delta (0.0–1.0)
  - option_premium (cost per share, multiply ×100 for per contract)
  - bid_ask_spread_pct (0.0–1.0, e.g. 0.08 = 8%)
  - open_interest
  - daily_volume_options
  - dte (days to expiration)
  - direction ("bullish" | "bearish")
"""

import os
import numpy as np
import pandas as pd
import datetime
from dataclasses import dataclass, field
from typing import Optional

from ap_intelligence.tools.ap_data_tools import get_prices, get_spy_trend, get_vix
from ap_intelligence.ap_mode_config import APModeConfig


# ─────────────────────────────────────────────
# INDEX TICKERS — exempt from SPY regime gate
# These ETFs/indices ARE the broad-market regime, so blocking a
# SPY PUT because "SPY is BULL" is circular. 232 setups on indices
# should be taken on their own technical merit regardless of regime.
# (Post-dedupe in master_control, ^GSPC/^NDX/^RUT/^DJI arrive as
# SPY/QQQ/IWM/DIA — matching those is sufficient.)
# ─────────────────────────────────────────────
INDEX_TICKERS = {"SPY", "QQQ", "IWM", "DIA"}

# ─────────────────────────────────────────────
# SECTOR / THEME BUCKETS
# Maps tickers to exposure buckets for concentration tracking.
# Add your full universe here.
# ─────────────────────────────────────────────
SECTOR_MAP = {
    # Tech
    "AAPL": "tech", "MSFT": "tech", "GOOGL": "tech", "META": "tech",
    "NVDA": "tech", "AMD": "tech", "INTC": "tech", "CRM": "tech",
    "ORCL": "tech", "ADBE": "tech", "QCOM": "tech", "AVGO": "tech",
    # Semis (sub-bucket of tech but tracked separately)
    "SMH":  "semis", "SOXX": "semis", "MU": "semis", "LRCX": "semis",
    # Consumer
    "AMZN": "consumer", "TSLA": "consumer", "NKE": "consumer",
    "HD": "consumer", "MCD": "consumer", "SBUX": "consumer",
    # Financials
    "JPM": "financial", "GS": "financial", "MS": "financial",
    "BAC": "financial", "WFC": "financial",
    # Indices / ETFs
    "SPY": "index", "QQQ": "index", "IWM": "index", "DIA": "index",
    # Healthcare
    "JNJ": "healthcare", "PFE": "healthcare", "MRNA": "healthcare",
    "UNH": "healthcare",
    # Energy
    "XOM": "energy", "CVX": "energy", "OXY": "energy",
}

# Max gross exposure per direction per sector (% of portfolio)
MAX_SECTOR_EXPOSURE = {
    "tech":       0.40,
    "semis":      0.25,
    "index":      0.50,
    "consumer":   0.30,
    "financial":  0.25,
    "healthcare": 0.20,
    "energy":     0.20,
    "other":      0.20,
}

# Max simultaneous open positions by direction
MAX_LONGS = 5
MAX_SHORTS = 5
MAX_SAME_SECTOR_POSITIONS = 2

# Contract quality hard gates
CONTRACT_FILTERS = {
    "max_spread_pct":   0.14,    # 14% — risk manager veto (per spec: 10%→14%)
    "min_open_interest":100,     # loosened from 500 — further gated in contract_selector
    "min_daily_volume": 30,      # loosened — further gated in contract_selector
    "min_delta":        0.20,    # allow slightly more OTM
    "max_delta":        0.75,    # allow slightly more ITM
    "min_dte":          1,
    "max_dte_0dte":     1,
}


@dataclass
class ContractQuality:
    passes: bool
    spread_ok: bool
    oi_ok: bool
    volume_ok: bool
    delta_ok: bool
    dte_ok: bool
    rejection_reason: str = ""


@dataclass
class RiskResult:
    ticker: str
    approved: bool
    max_contracts: int
    max_position_usd: float         # Total premium cost cap
    risk_dollars: float             # Account risk on this trade
    stop_distance_pct: float        # Expected underlying move to stop
    expected_loss_per_contract: float  # Option loss at stop
    volatility_pct: float
    position_limit_pct: float
    correlation_multiplier: float
    sector_multiplier: float
    contract_quality: ContractQuality
    spy_trend: str
    vix: float
    reason: str
    # Structured execution-authority contract.
    #
    # hard_veto=False means the result is approved/advisory and must not be
    # converted into an execution-authoritative RISK_VETO by the bridge.
    #
    # hard_veto=True means a genuine risk/safety/capital gate failed.
    #
    # reason_code is a stable machine-readable classification.
    # veto_category is a stable reporting bucket and must never itself decide
    # eligibility.
    reason_code: str = ""
    veto_category: str = ""
    hard_veto: bool = False
    # Gate G may run before a selected OCC contract or canonical account
    # snapshot exists.  These fields keep that authority boundary explicit in
    # the result instead of making callers infer it from default values.
    contract_quality_authoritative: bool = True
    account_state_authoritative: bool = True
    authority_diagnostics: dict = field(default_factory=dict)


class APRiskManager:
    """
    Angel Precision Risk Manager v2 — TRUE STOP-BASED SIZING.

    Sizing formula:
        risk_dollars = portfolio_value × risk_pct_per_trade
        expected_loss_per_contract = option_premium × delta × stop_pct × 100
        contracts = floor(risk_dollars / expected_loss_per_contract)

    Then apply caps:
        - vol-adjusted position cap
        - sector exposure cap
        - correlation multiplier
        - contract liquidity cap
        - daily loss kill switch

    NO confidence scaling here. Confidence lives in portfolio manager.
    """

    def __init__(
        self,
        portfolio_value: float,
        risk_pct_per_trade: float = 0.10,       # 10% account risk per trade — aligns with MAX_TRADE_USD
        max_position_pct: float = 0.10,         # 10% position cap — aligns with MAX_TRADE_USD
        daily_loss_limit_pct: float = -0.05,    # Kill switch at -5% day
        allow_0dte: bool = True,
        mode: str = None,
        max_contracts_hard_cap: int = 15,       # mirrors ap.execution / contract_selector MAX_CONTRACTS (PR #30)
    ):
        self.portfolio_value        = portfolio_value
        self.risk_pct_per_trade     = risk_pct_per_trade
        self.max_position_pct       = max_position_pct
        self.daily_loss_limit_pct   = daily_loss_limit_pct
        self.allow_0dte             = allow_0dte
        self.mode_cfg               = APModeConfig(mode=mode)
        self.max_contracts_hard_cap = max_contracts_hard_cap

        # Exposure tracking
        self.open_positions: dict[str, dict] = {}
        # {ticker: {direction: "bullish"|"bearish", cost_usd: float, sector: str}}

        self.daily_pnl: float = 0.0

        # Returns cache for correlation — ticker → pd.Series (aligned, cleaned once)
        self._returns_cache: dict[str, pd.Series] = {}
        self._cache_date: str = ""  # reset daily

    # ────────────────────────────────────────────
    # MAIN EVALUATION
    # ────────────────────────────────────────────
    def evaluate(
        self,
        ticker: str,
        direction: str,               # "bullish" | "bearish"
        underlying_price: float,
        atr_value: float,             # ATR of underlying (from your scanner)
        option_delta: float,          # 0.0–1.0
        option_premium: float,        # Cost per share (premium, NOT × 100 yet)
        bid_ask_spread_pct: float = 0.05,
        open_interest: int = 1000,
        daily_volume_options: int = 500,
        dte: int = 1,
        atr_stop_multiple: float = 1.0,  # Number of ATRs to stop distance
        contract_quality_authoritative: bool = True,
        account_state_authoritative: bool = True,
        authority_diagnostics: Optional[dict] = None,
    ) -> RiskResult:

        authority_diagnostics = dict(authority_diagnostics or {})
        authority_diagnostics.setdefault(
            "contract_quality_authoritative",
            bool(contract_quality_authoritative),
        )
        authority_diagnostics.setdefault(
            "account_state_authoritative",
            bool(account_state_authoritative),
        )

        # ── 0. Daily kill switch ──────────────────────────────
        if account_state_authoritative and self.daily_pnl < (
            self.portfolio_value * self.daily_loss_limit_pct
        ):
            return self._reject(
                ticker,
                direction,
                option_premium,
                dte,
                bid_ask_spread_pct,
                open_interest,
                daily_volume_options,
                option_delta,
                f"DAILY KILL SWITCH: P&L = ${self.daily_pnl:.0f}",
                reason_code="DAILY_LOSS_KILL_SWITCH",
                veto_category="ACCOUNT_SAFETY",
                hard_veto=True,
                authority_diagnostics=authority_diagnostics,
                contract_quality_authoritative=contract_quality_authoritative,
                account_state_authoritative=account_state_authoritative,
            )
        if not account_state_authoritative:
            authority_diagnostics["daily_pnl"] = {
                "value": self.daily_pnl,
                "source": "process_local_APRiskManager",
                "classification": "UNAVAILABLE",
                "authoritative": False,
                "reason": "canonical account/day P&L snapshot not supplied",
            }

        # ── 1. Market regime context + VIX hard gate ───────────
        spy = get_spy_trend()
        vix_data = get_vix()

        spy_trend = str((spy or {}).get("trend") or "UNKNOWN").upper()
        vix_value = (vix_data or {}).get("vix")
        authority_diagnostics["vix"] = {
            "value": vix_value,
            "source": (vix_data or {}).get("source", "ap_data_tools.get_vix"),
            "source_ts": (vix_data or {}).get("observed_at"),
            "classification": (vix_data or {}).get(
                "classification", "PRODUCTION_EXACT"
            ),
            "authoritative": (vix_data or {}).get("tradeable") is True,
        }

        # Index tickers are themselves broad-market instruments. Preserve the
        # existing exemption, but still record SPY context in RiskResult.
        is_index = ticker.upper() in INDEX_TICKERS

        # SPY trend is a broad, multi-session context measurement. A disagreement
        # between that measurement and an individual scanner setup is advisory.
        # It must not return early or prevent the remaining hard gates from
        # running.
        regime_mismatch = bool(
            not is_index
            and (
                (direction == "bullish" and spy_trend == "BEAR")
                or
                (direction == "bearish" and spy_trend == "BULL")
            )
        )

        regime_reason = ""
        if regime_mismatch:
            regime_reason = (
                "SPY regime mismatch advisory: "
                f"{direction.upper()} setup while SPY 20-day trend={spy_trend}"
            )

        if is_index and (
            (direction == "bullish" and spy_trend == "BEAR")
            or
            (direction == "bearish" and spy_trend == "BULL")
        ):
            import logging as _lg

            _lg.getLogger(__name__).info(
                "[%s] Index regime exemption — %s allowed despite SPY trend=%s "
                "(index setup judged on its own technical merit)",
                ticker,
                direction,
                spy_trend,
            )

        if (vix_data or {}).get("tradeable") is not True:
            return self._reject(
                ticker,
                direction,
                option_premium,
                dte,
                bid_ask_spread_pct,
                open_interest,
                daily_volume_options,
                option_delta,
                f"VIX={vix_value} outside 12-35",
                spy_trend=spy_trend,
                vix=vix_value,
                reason_code="VIX_POLICY_HARD_CAP",
                veto_category="MARKET_SAFETY",
                hard_veto=True,
                authority_diagnostics=authority_diagnostics,
                contract_quality_authoritative=contract_quality_authoritative,
                account_state_authoritative=account_state_authoritative,
            )

        # ── 2. Contract quality filter (HARD GATE) ────────────
        cq = self._contract_quality(
            spread_pct=bid_ask_spread_pct,
            open_interest=open_interest,
            daily_volume=daily_volume_options,
            delta=option_delta,
            dte=dte,
        )
        authority_diagnostics["contract_quality"] = {
            "value": {
                "passes": cq.passes,
                "spread_ok": cq.spread_ok,
                "oi_ok": cq.oi_ok,
                "volume_ok": cq.volume_ok,
                "delta_ok": cq.delta_ok,
                "dte_ok": cq.dte_ok,
            },
            "source": "selected_contract" if contract_quality_authoritative else "pipeline_defaults_or_signal_metadata",
            "classification": "PRODUCTION_EXACT" if contract_quality_authoritative else "ESTIMATED_ADVISORY",
            "authoritative": bool(contract_quality_authoritative),
            "reason": cq.rejection_reason or "passed",
        }

        if not cq.passes and contract_quality_authoritative:
            return self._reject(
                ticker,
                direction,
                option_premium,
                dte,
                bid_ask_spread_pct,
                open_interest,
                daily_volume_options,
                option_delta,
                f"CONTRACT QUALITY: {cq.rejection_reason}",
                spy_trend=spy_trend,
                vix=vix_value,
                contract_quality=cq,
                reason_code="CONTRACT_QUALITY_FAILED",
                veto_category="EXECUTION_QUALITY",
                hard_veto=True,
                authority_diagnostics=authority_diagnostics,
                contract_quality_authoritative=contract_quality_authoritative,
                account_state_authoritative=account_state_authoritative,
            )

        # Gate G is intentionally not a second account-risk authority.  When
        # Master Control has not supplied a canonical account snapshot, stop
        # before the legacy manager's process-local exposure/capital/sizing
        # calculations.  Keep genuine market safety (VIX) and, when present,
        # exact selected-contract quality above; everything else is advisory.
        if not account_state_authoritative:
            authority_diagnostics["account_state"] = {
                "value": None,
                "source": "APRiskManager.process_local_state",
                "classification": "UNAVAILABLE",
                "authoritative": False,
                "reason": "canonical Master Control snapshot is the account-risk authority",
            }
            authority_diagnostics["sector_exposure"] = {
                "value": None,
                "source": "APRiskManager.open_positions",
                "classification": "UNAVAILABLE",
                "authoritative": False,
                "reason": "process-local portfolio state is not current account truth",
            }
            authority_diagnostics["sizing"] = {
                "value": None,
                "source": "APRiskManager",
                "classification": "UNAVAILABLE",
                "authoritative": False,
                "reason": "contract cost and account equity are not Gate G authority pre-selector",
            }
            _advisory_reason = (
                f"{regime_reason}; " if regime_reason else ""
            ) + "account/portfolio risk advisory: canonical snapshot unavailable"
            return RiskResult(
                ticker=ticker,
                approved=True,
                max_contracts=1,
                max_position_usd=0.0,
                risk_dollars=0.0,
                stop_distance_pct=0.0,
                expected_loss_per_contract=0.0,
                volatility_pct=0.0,
                position_limit_pct=0.0,
                correlation_multiplier=1.0,
                sector_multiplier=1.0,
                contract_quality=cq,
                spy_trend=spy_trend,
                vix=vix_value,
                reason=_advisory_reason,
                reason_code=(
                    "APPROVED_WITH_REGIME_MISMATCH"
                    if regime_mismatch
                    else "ADVISORY_DATA_UNAVAILABLE"
                ),
                veto_category="MARKET_CONTEXT" if regime_mismatch else "DATA_AVAILABILITY",
                hard_veto=False,
                contract_quality_authoritative=bool(contract_quality_authoritative),
                account_state_authoritative=False,
                authority_diagnostics=authority_diagnostics,
            )

        # ── 3. Exposure bucket check ──────────────────────────
        sector = SECTOR_MAP.get(ticker, "other")
        sector_mult = self._sector_multiplier(ticker, direction, sector)
        if sector_mult == 0.0:
            return self._reject(
                ticker,
                direction,
                option_premium,
                dte,
                bid_ask_spread_pct,
                open_interest,
                daily_volume_options,
                option_delta,
                f"SECTOR CAP: {sector} exposure limit reached",
                spy_trend=spy_trend,
                vix=vix_value,
                contract_quality=cq,
                reason_code="SECTOR_EXPOSURE_CAP",
                veto_category="PORTFOLIO_EXPOSURE",
                hard_veto=True,
                authority_diagnostics=authority_diagnostics,
                contract_quality_authoritative=contract_quality_authoritative,
                account_state_authoritative=account_state_authoritative,
            )

        # ── 4. Volatility-adjusted position cap ───────────────
        end   = datetime.date.today().strftime("%Y-%m-%d")
        start = (datetime.date.today() - datetime.timedelta(days=70)).strftime("%Y-%m-%d")
        df    = get_prices(ticker, start, end)
        vol_metrics = self._calc_volatility(ticker, df)
        vol_cap_pct = self._vol_cap(vol_metrics["annualized"])

        # ── 5. Correlation multiplier ─────────────────────────
        corr_mult = self._correlation_multiplier(ticker)

        # ── 6. TRUE STOP-BASED SIZING ─────────────────────────
        # Risk budget for this trade
        risk_dollars = self.portfolio_value * self.risk_pct_per_trade

        # Stop distance in underlying price terms
        # Your scanner provides ATR — use atr_stop_multiple × ATR as stop
        stop_distance_usd = atr_value * atr_stop_multiple
        stop_pct = stop_distance_usd / underlying_price if underlying_price > 0 else 0.05

        # Expected option loss at stop = premium × delta × stop_pct × 100 shares
        # delta approximates how much the option moves per $1 underlying move
        # stop_pct × underlying_price = underlying move to stop
        underlying_move_to_stop = stop_pct * underlying_price
        expected_loss_per_contract = option_delta * underlying_move_to_stop * 100

        # Contracts from risk budget
        if expected_loss_per_contract > 0:
            contracts_from_risk = int(risk_dollars / expected_loss_per_contract)
        else:
            # Risk math failed (zero denominator) — floor at 2 so accounts
            # that can afford 2+ contracts don't get capped to 1 on a data gap.
            # Capital and dollar caps downstream will constrain if budget is tight.
            contracts_from_risk = 2

        # ── 7. Apply all caps ──────────────────────────────────
        # Cap 1: Vol + sector + correlation adjusted position cap
        combined_cap_pct = vol_cap_pct * corr_mult * sector_mult
        combined_cap_pct = max(0.03, min(self.max_position_pct, combined_cap_pct))
        dollar_cap = self.portfolio_value * combined_cap_pct

        # Cap 2: Premium cost cap (total cost of contracts)
        cost_per_contract = option_premium * 100
        if cost_per_contract > 0:
            contracts_from_dollar_cap = int(dollar_cap / cost_per_contract)
        else:
            contracts_from_dollar_cap = 0

        # Cap 3: Available capital
        already_deployed = sum(p["cost_usd"] for p in self.open_positions.values())
        remaining = max(0.0, (self.portfolio_value * 0.80) - already_deployed)  # Keep 20% cash
        contracts_from_capital = int(remaining / cost_per_contract) if cost_per_contract > 0 else 0

        # Final contracts = minimum of all constraints + hard cap
        max_contracts = max(0, min(
            contracts_from_risk,
            contracts_from_dollar_cap,
            contracts_from_capital,
            self.max_contracts_hard_cap,   # hard ceiling — matches contract_selector MAX_CONTRACTS
        ))
        max_usd = max_contracts * cost_per_contract

        approved = max_contracts >= 1

        if not approved:
            return self._reject(
                ticker,
                direction,
                option_premium,
                dte,
                bid_ask_spread_pct,
                open_interest,
                daily_volume_options,
                option_delta,
                "Insufficient capital or zero contracts",
                spy_trend=spy_trend,
                vix=vix_value,
                contract_quality=cq,
                reason_code="INSUFFICIENT_CAPITAL_OR_ZERO_CONTRACTS",
                veto_category="CAPITAL",
                hard_veto=True,
            )

        return RiskResult(
            ticker=ticker,
            approved=True,
            max_contracts=max_contracts,
            max_position_usd=round(max_usd, 2),
            risk_dollars=round(risk_dollars, 2),
            stop_distance_pct=round(stop_pct * 100, 2),
            expected_loss_per_contract=round(expected_loss_per_contract, 2),
            volatility_pct=round(vol_metrics["annualized"] * 100, 2),
            position_limit_pct=round(combined_cap_pct * 100, 2),
            correlation_multiplier=round(corr_mult, 3),
            sector_multiplier=round(sector_mult, 3),
            contract_quality=cq,
            spy_trend=spy_trend,
            vix=vix_value,
            reason=regime_reason or "APPROVED",
            reason_code=(
                "APPROVED_WITH_REGIME_MISMATCH"
                if regime_mismatch
                else "APPROVED"
            ),
            veto_category=(
                "MARKET_CONTEXT"
                if regime_mismatch
                else "NONE"
            ),
            hard_veto=False,
        )

    # ────────────────────────────────────────────
    # CONTRACT QUALITY FILTER
    # ────────────────────────────────────────────
    def _contract_quality(
        self,
        spread_pct: float,
        open_interest: int,
        daily_volume: int,
        delta: float,
        dte: int,
    ) -> ContractQuality:
        # Use mode-aware contract config (research mode loosens OI/vol/delta slightly)
        f = self.mode_cfg.contracts
        spread_ok  = spread_pct <= f.max_spread_pct
        oi_ok      = open_interest >= f.min_open_interest
        vol_ok     = daily_volume >= f.min_daily_volume
        delta_ok   = f.min_delta <= abs(delta) <= f.max_delta
        dte_ok     = dte >= f.min_dte or (dte == 0 and self.allow_0dte)

        fails = []
        if not spread_ok:  fails.append(f"spread {spread_pct:.1%} > {f.max_spread_pct:.0%}")
        if not oi_ok:      fails.append(f"OI {open_interest} < {f.min_open_interest}")
        if not vol_ok:     fails.append(f"vol {daily_volume} < {f.min_daily_volume}")
        if not delta_ok:   fails.append(f"delta {delta:.2f} outside [{f.min_delta},{f.max_delta}]")
        if not dte_ok:     fails.append(f"DTE={dte} too short")

        passes = len(fails) == 0
        return ContractQuality(
            passes=passes,
            spread_ok=spread_ok,
            oi_ok=oi_ok,
            volume_ok=vol_ok,
            delta_ok=delta_ok,
            dte_ok=dte_ok,
            rejection_reason=", ".join(fails) if fails else "",
        )

    # ────────────────────────────────────────────
    # EXPOSURE BUCKET (SECTOR / DIRECTION)
    # ────────────────────────────────────────────
    def _sector_multiplier(self, ticker: str, direction: str, sector: str) -> float:
        """
        Returns a multiplier 0.0–1.0 based on sector/direction concentration.
        Returns 0.0 = hard block (cap hit).
        """
        # Count open positions by direction
        longs  = sum(1 for p in self.open_positions.values() if p["direction"] == "bullish")
        shorts = sum(1 for p in self.open_positions.values() if p["direction"] == "bearish")

        if direction == "bullish" and longs >= MAX_LONGS:
            return 0.0
        if direction == "bearish" and shorts >= MAX_SHORTS:
            return 0.0

        # Count same-sector open positions
        sector_count = sum(
            1 for t, p in self.open_positions.items()
            if p.get("sector") == sector and t != ticker
        )
        if sector_count >= MAX_SAME_SECTOR_POSITIONS:
            return 0.0

        # Measure current sector dollar exposure
        sector_exposure = sum(
            p["cost_usd"] for t, p in self.open_positions.items()
            if p.get("sector") == sector and p.get("direction") == direction
        )
        sector_cap = self.portfolio_value * MAX_SECTOR_EXPOSURE.get(sector, 0.20)

        if sector_exposure >= sector_cap:
            return 0.0

        # Soft reduction as approaching cap
        remaining_sector_pct = (sector_cap - sector_exposure) / sector_cap
        if remaining_sector_pct < 0.3:
            return 0.5   # Near cap → half size
        return 1.0

    # ────────────────────────────────────────────
    # CORRELATION (CLEAN + RELIABLE)
    # ────────────────────────────────────────────
    def _correlation_multiplier(self, ticker: str) -> float:
        """
        Clean correlation: refresh cache daily, align series once,
        compare new ticker vs all open positions.
        """
        today = datetime.date.today().strftime("%Y-%m-%d")

        # Reset cache daily
        if self._cache_date != today:
            self._returns_cache = {}
            self._cache_date = today

        # Need at least one open position to compare against
        open_tickers = list(self.open_positions.keys())
        if not open_tickers:
            return 1.0

        # Load returns for ticker if not cached
        if ticker not in self._returns_cache:
            df = self._fetch_returns(ticker)
            if df is not None:
                self._returns_cache[ticker] = df

        # Load returns for all open positions if not cached
        for t in open_tickers:
            if t not in self._returns_cache:
                df = self._fetch_returns(t)
                if df is not None:
                    self._returns_cache[t] = df

        # Need the new ticker + at least one open ticker in cache
        if ticker not in self._returns_cache:
            return 1.0

        comparable = [t for t in open_tickers if t in self._returns_cache]
        if not comparable:
            return 1.0

        # Build aligned DataFrame — drop all NaN rows for clean correlation
        all_series = {ticker: self._returns_cache[ticker]}
        for t in comparable:
            all_series[t] = self._returns_cache[t]

        try:
            ret_df = pd.DataFrame(all_series).dropna(how="any")
            if ret_df.shape[0] < 10 or ticker not in ret_df.columns:
                return 1.0

            corr_matrix = ret_df.corr()
            corr_values = corr_matrix.loc[ticker, comparable].dropna()
            if corr_values.empty:
                return 1.0
            avg_corr = float(corr_values.mean())
        except Exception:
            return 1.0

        # Map avg correlation → multiplier
        if avg_corr >= 0.80: return 0.60   # Highly correlated cluster → 60% size
        if avg_corr >= 0.65: return 0.75
        if avg_corr >= 0.50: return 0.90
        if avg_corr >= 0.30: return 1.00
        return 1.10                          # Low correlation → slight bonus

    def _fetch_returns(self, ticker: str) -> Optional[pd.Series]:
        end   = datetime.date.today().strftime("%Y-%m-%d")
        start = (datetime.date.today() - datetime.timedelta(days=70)).strftime("%Y-%m-%d")
        df = get_prices(ticker, start, end)
        if df.empty or len(df) < 10 or "close" not in df.columns:
            return None
        return df["close"].pct_change().dropna().rename(ticker)

    # ────────────────────────────────────────────
    # VOLATILITY CAP
    # ────────────────────────────────────────────
    def _calc_volatility(self, ticker: str, df: pd.DataFrame) -> dict:
        if df.empty or len(df) < 5 or "close" not in df.columns:
            return {"annualized": 0.40}
        returns = df["close"].pct_change().dropna()
        daily_vol = float(returns.tail(60).std())
        ann_vol = daily_vol * np.sqrt(252)
        self._returns_cache[ticker] = returns  # Cache while we have it
        return {"annualized": ann_vol if not np.isnan(ann_vol) else 0.40}

    def _vol_cap(self, ann_vol: float) -> float:
        """Volatility → max position as % of portfolio. NO confidence scaling."""
        base = 0.20
        if ann_vol < 0.15:   return base * 1.00   # 20%
        if ann_vol < 0.30:   return base * 0.85   # 17%
        if ann_vol < 0.50:   return base * 0.65   # 13%
        return base * 0.50                          # 10% for very high vol

    # ────────────────────────────────────────────
    # POSITION TRACKING
    # ────────────────────────────────────────────
    def record_open(self, ticker: str, direction: str, cost_usd: float):
        """Call when a position is opened."""
        sector = SECTOR_MAP.get(ticker, "other")
        self.open_positions[ticker] = {
            "direction": direction,
            "cost_usd":  cost_usd,
            "sector":    sector,
            "opened":    datetime.datetime.now().isoformat(),
        }

    def record_close(self, ticker: str, pnl: float):
        """Call when a position is closed."""
        self.open_positions.pop(ticker, None)
        self.daily_pnl += pnl

    def reset_daily(self):
        self.daily_pnl = 0.0

    # ────────────────────────────────────────────
    # HELPERS
    # ────────────────────────────────────────────
    def _reject(
        self,
        ticker,
        direction,
        premium,
        dte,
        spread,
        oi,
        vol,
        delta,
        reason,
        spy_trend="UNKNOWN",
        vix=0.0,
        contract_quality=None,
        reason_code: str = "UNCLASSIFIED_RISK_REJECTION",
        veto_category: str = "RISK",
        hard_veto: bool = True,
        authority_diagnostics: Optional[dict] = None,
        contract_quality_authoritative: bool = True,
        account_state_authoritative: bool = True,
    ) -> RiskResult:
        if contract_quality is None:
            contract_quality = self._contract_quality(
                spread,
                oi,
                vol,
                delta,
                dte,
            )

        return RiskResult(
            ticker=ticker,
            approved=False,
            max_contracts=0,
            max_position_usd=0.0,
            risk_dollars=0.0,
            stop_distance_pct=0.0,
            expected_loss_per_contract=0.0,
            volatility_pct=0.0,
            position_limit_pct=0.0,
            correlation_multiplier=1.0,
            sector_multiplier=1.0,
            contract_quality=contract_quality,
            spy_trend=spy_trend,
            vix=vix,
            reason=reason,
            reason_code=reason_code,
            veto_category=veto_category,
            hard_veto=hard_veto,
            contract_quality_authoritative=bool(contract_quality_authoritative),
            account_state_authoritative=bool(account_state_authoritative),
            authority_diagnostics=dict(authority_diagnostics or {}),
        )
