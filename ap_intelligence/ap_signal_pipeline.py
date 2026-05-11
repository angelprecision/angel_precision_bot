"""
ap_signal_pipeline.py — Angel Precision Full Signal Pipeline v2
================================================================
Orchestrates all AP agents into a single call.

This is the master file that wires together:
1. Your existing scanner (Strat patterns)
2. Technical Agent (5-strategy confirmation)
3. Sentiment Agent (news + insider + short interest) — OPTIONAL
4. Fundamentals Agent (quality context filter) — OPTIONAL
5. Risk Manager v2 (true stop-based sizing, exposure buckets, contract gates)
6. Portfolio Manager v2 (scorecard 0-100, hard size tiers)
7. Audit Log (every decision recorded with full why)

v2 CHANGES
──────────
- run() now accepts all v2 risk manager inputs:
    atr_value, option_delta, bid_ask_spread_pct,
    open_interest, daily_volume_options, dte
- option_cost_per_contract REPLACED by option_premium (per share, not ×100)
- Sentiment defaults to neutral if slow/unavailable — never blocks execution
- Audit log automatically records every decision
- run_quick() for fast scanner integration (minimal required params)

Usage
─────
    from ap_intelligence.ap_signal_pipeline import APSignalPipeline

    pipeline = APSignalPipeline(
        portfolio_value=10000,
        openai_api_key="sk-...",   # optional
    )

    # Full call with all contract data
    result = pipeline.run(
        ticker="NVDA",
        scanner_signal="bullish",
        scanner_confidence=88,
        underlying_price=950.00,
        atr_value=18.5,             # From ap_scanner_utils.atr_stop()
        option_premium=3.50,        # Per share (×100 = per contract cost)
        option_delta=0.45,
        bid_ask_spread_pct=0.06,    # 6%
        open_interest=1200,
        daily_volume_options=350,
        dte=1,
    )

    # Quick call — contract defaults applied, use for fast scanning
    result = pipeline.run_quick(
        ticker="AAPL",
        scanner_signal="bullish",
        scanner_confidence=82,
        underlying_price=185.0,
    )

    pipeline.send_discord(result, webhook_url="https://discord.com/api/webhooks/...")
"""

import os
import json
import datetime
import requests

from ap_intelligence.agents.ap_technical_agent    import APTechnicalAgent
from ap_intelligence.agents.ap_sentiment_agent    import APSentimentAgent
from ap_intelligence.agents.ap_fundamentals_agent import APFundamentalsAgent
from ap_intelligence.agents.ap_risk_manager       import APRiskManager
from ap_intelligence.agents.ap_portfolio_manager  import (
    APPortfolioManager,
    PortfolioDecision,
    format_decision_for_discord,
)
from ap_intelligence.tools.ap_data_tools          import get_prices
from ap_intelligence.ap_audit_log                 import APAuditLog


class APSignalPipeline:
    """
    Full Angel Precision Intelligence Stack v2.
    Run all agents → get a final trade decision in one call.
    """

    def __init__(
        self,
        portfolio_value:  float = 10_000,
        openai_api_key:   str   = None,
        use_llm:          bool  = True,
        use_sentiment:    bool  = True,
        use_fundamentals: bool  = True,
        audit_log:        APAuditLog = None,
    ):
        self.portfolio_value  = portfolio_value
        self.openai_api_key   = openai_api_key or os.environ.get("OPENAI_API_KEY", "")
        self.use_sentiment    = use_sentiment
        self.use_fundamentals = use_fundamentals

        # Initialize agents
        self.technical    = APTechnicalAgent()
        self.sentiment    = APSentimentAgent(openai_api_key=self.openai_api_key)
        self.fundamentals = APFundamentalsAgent()
        self.risk_manager = APRiskManager(
            portfolio_value=portfolio_value,
            risk_pct_per_trade=float(os.getenv("RISK_PCT_PER_TRADE", "0.10")),  # 10% = aligns with MAX_TRADE_USD=1800
            max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.10")),       # 10% hard position cap
            max_contracts_hard_cap=int(os.getenv("MAX_CONTRACTS", "6")),          # mirrors contract_selector cap
        )
        self.pm           = APPortfolioManager(
            openai_api_key=self.openai_api_key,
            use_llm=use_llm,
        )
        self.audit        = audit_log or APAuditLog()

    # ──────────────────────────────────────────────────────────────────────
    # FULL PIPELINE  (all v2 params)
    # ──────────────────────────────────────────────────────────────────────

    def run(
        self,
        ticker:               str,
        scanner_signal:       str,           # "bullish" | "bearish" | "neutral"
        scanner_confidence:   float,         # 0-100
        underlying_price:     float,
        # ── v2 risk manager inputs ───────────────────────────────────────
        atr_value:            float  = None, # ATR of underlying — ap_scanner_utils.atr_stop()
        option_premium:       float  = None, # Per share (NOT ×100) — e.g. 3.50
        option_delta:         float  = 0.45, # 0.0–1.0
        bid_ask_spread_pct:   float  = 0.06, # 0.0–1.0  (e.g. 0.06 = 6%)
        open_interest:        int    = 500,
        daily_volume_options: int    = 100,
        dte:                  int    = 1,
        atr_stop_multiple:    float  = 1.0,
        allow_0dte:           bool   = True,
        # ── legacy compat ────────────────────────────────────────────────
        option_cost_per_contract: float = None,  # DEPRECATED — use option_premium
        lookback_days:        int    = 60,
    ) -> dict:
        """
        Run the full pipeline and return a complete decision packet.

        Parameters
        ----------
        ticker               : Symbol to evaluate
        scanner_signal       : Direction from scanner ("bullish"|"bearish"|"neutral")
        scanner_confidence   : Scanner confidence score 0-100
        underlying_price     : Current price of the underlying stock
        atr_value            : ATR value from ap_scanner_utils.atr_stop()
                               If None, estimated as 1.5% of underlying_price
        option_premium       : Option ask price per share (multiply ×100 for contract cost)
                               If None, estimated as 1% of underlying_price
        option_delta         : Option delta 0-1 (default 0.45 — near-the-money)
        bid_ask_spread_pct   : Spread as fraction of mid price (e.g. 0.06 = 6%)
        open_interest        : Open interest contracts
        daily_volume_options : Daily option volume contracts
        dte                  : Days to expiration
        atr_stop_multiple    : ATR multiples for stop distance (default 1.0)
        allow_0dte           : Whether 0DTE contracts are allowed (default True)
        option_cost_per_contract : DEPRECATED — if provided, converts to option_premium

        Returns
        -------
        dict with keys: ticker, action, direction, contracts, max_usd,
                        confidence, score, reasoning, signal_breakdown,
                        tech_breakdown, risk_detail, timestamp
        """

        timestamp = datetime.datetime.now().isoformat()

        # ── Handle legacy option_cost_per_contract ────────────────────────
        if option_cost_per_contract is not None and option_premium is None:
            option_premium = option_cost_per_contract / 100.0

        # ── Derive defaults if not provided ───────────────────────────────
        if atr_value is None:
            atr_value = underlying_price * 0.015       # ~1.5% daily ATR estimate
        if option_premium is None:
            option_premium = underlying_price * 0.01   # ~1% OTM estimate

        # ── 1. Get price data ─────────────────────────────────────────────
        end   = datetime.date.today().strftime("%Y-%m-%d")
        start = (datetime.date.today() - datetime.timedelta(
            days=lookback_days + 10
        )).strftime("%Y-%m-%d")
        df = get_prices(ticker, start, end)

        # ── 2. Technical Agent ────────────────────────────────────────────
        tech_result = self.technical.analyze(
            ticker=ticker,
            df=df,
            strat_signal=scanner_signal,
        )

        # ── 3. Sentiment Agent — OPTIONAL, never blocks ───────────────────
        if self.use_sentiment:
            try:
                sent_result = self.sentiment.analyze(ticker)
            except Exception:
                sent_result = {"signal": "neutral", "confidence": 50}
        else:
            sent_result = {"signal": "neutral", "confidence": 50}

        # ── 4. Fundamentals Agent — OPTIONAL context filter ───────────────
        if self.use_fundamentals:
            try:
                fund_result = self.fundamentals.analyze(ticker)
            except Exception:
                fund_result = {"signal": "neutral", "confidence": 50}
        else:
            fund_result = {"signal": "neutral", "confidence": 50}

        # ── 5. Risk Manager v2 ────────────────────────────────────────────
        risk_result = self.risk_manager.evaluate(
            ticker                = ticker,
            direction             = scanner_signal,
            underlying_price      = underlying_price,
            atr_value             = atr_value,
            option_delta          = option_delta,
            option_premium        = option_premium,
            bid_ask_spread_pct    = bid_ask_spread_pct,
            open_interest         = open_interest,
            daily_volume_options  = daily_volume_options,
            dte                   = dte,
            atr_stop_multiple     = atr_stop_multiple,
            # allow_0dte removed — APRiskManager stores it on self, not evaluate()
        )

        # ── Build signals dict for portfolio manager ───────────────────────
        signals = {
            "scanner": {
                "signal":     scanner_signal,
                "confidence": scanner_confidence,
            },
            "technical": {
                "signal":     tech_result["signal"],
                "confidence": tech_result["confidence"],
            },
            "sentiment": {
                "signal":     sent_result.get("signal", "neutral"),
                "confidence": sent_result.get("confidence", 50),
            },
            "fundamentals": {
                "signal":     fund_result.get("signal", "neutral"),
                "confidence": fund_result.get("confidence", 50),
            },
            "risk": {
                "approved":           risk_result.approved,
                "reason":             risk_result.reason,
                "max_contracts":      risk_result.max_contracts,
                "max_position_usd":   risk_result.max_position_usd,
                "spy_trend":          risk_result.spy_trend,
                "vix":                risk_result.vix,
                "vol_pct":            risk_result.volatility_pct,
                "position_limit_pct": risk_result.position_limit_pct,
                # v2 additions
                "stop_price":         getattr(risk_result, "stop_price", None),
                "risk_per_contract":  getattr(risk_result, "risk_per_contract", None),
                "contract_quality":   getattr(risk_result, "contract_quality_passed", None),
            },
        }

        # ── 6. Portfolio Manager v2 ────────────────────────────────────────
        decision = self.pm.decide(ticker=ticker, signals=signals)

        # ── 7. Audit Log ──────────────────────────────────────────────────
        self.audit.record({
            "ticker":          ticker,
            "timestamp":       timestamp,
            "action":          decision.action,
            "direction":       decision.direction,
            "scanner_signal":  scanner_signal,
            "scanner_conf":    scanner_confidence,
            "score_breakdown": decision.signal_breakdown,
            "signal_breakdown": decision.signal_breakdown,
            "risk_approved":   risk_result.approved,
            "risk_reason":     risk_result.reason,
            "contracts":       decision.contracts,
            "max_usd":         decision.max_usd,
            "reasoning":       decision.reasoning,
            "regime": {
                "spy_trend": risk_result.spy_trend,
                "vix":       risk_result.vix,
            },
            "contract_data": {
                "dte":                dte,
                "delta":              option_delta,
                "spread_pct":         bid_ask_spread_pct,
                "open_interest":      open_interest,
                "daily_volume":       daily_volume_options,
                "option_premium":     option_premium,
                "atr_value":          atr_value,
                "stop_price":         getattr(risk_result, "stop_price", None),
            },
            "mode":            self.mode_cfg.mode,
        })

        return {
            "ticker":          ticker,
            "timestamp":       timestamp,
            "action":          decision.action,
            "direction":       decision.direction,
            "contracts":       decision.contracts,
            "max_usd":         decision.max_usd,
            "confidence":      decision.confidence,
            "score":           getattr(decision, "score", None),
            "mode":            self.mode_cfg.mode,
            "reasoning":       decision.reasoning,
            "signal_breakdown":decision.signal_breakdown,
            "tech_breakdown":  tech_result.get("breakdown", {}),
            "risk_detail": {
                "approved":                risk_result.approved,
                "reason":                  risk_result.reason,
                "stop_price":              getattr(risk_result, "stop_price", None),
                "risk_per_contract":       getattr(risk_result, "risk_per_contract", None),
                "max_contracts":           risk_result.max_contracts,
                "max_position_usd":        risk_result.max_position_usd,
                "spy_trend":               risk_result.spy_trend,
                "vix":                     risk_result.vix,
                "contract_quality_passed": getattr(risk_result, "contract_quality_passed", None),
            },
        }

    # ──────────────────────────────────────────────────────────────────────
    # QUICK CALL  (minimal params — fast scanner integration)
    # ──────────────────────────────────────────────────────────────────────

    def run_quick(
        self,
        ticker:             str,
        scanner_signal:     str,
        scanner_confidence: float,
        underlying_price:   float,
        dte:                int   = 1,
        allow_0dte:         bool  = True,
    ) -> dict:
        """
        Minimal-param pipeline call.  All contract params estimated from
        underlying_price.  Use when you don't have live option chain data.

        Best for: fast initial scan to decide if a full call is warranted.
        """
        return self.run(
            ticker               = ticker,
            scanner_signal       = scanner_signal,
            scanner_confidence   = scanner_confidence,
            underlying_price     = underlying_price,
            dte                  = dte,
            allow_0dte           = allow_0dte,
        )

    # ──────────────────────────────────────────────────────────────────────
    # DISCORD
    # ──────────────────────────────────────────────────────────────────────

    def send_discord(self, result: dict, webhook_url: str) -> bool:
        """Post the pipeline result to a Discord channel."""
        decision = PortfolioDecision(
            ticker          = result["ticker"],
            action          = result["action"],
            direction       = result["direction"],
            contracts       = result["contracts"],
            max_usd         = result["max_usd"],
            confidence      = result["confidence"],
            reasoning       = result["reasoning"],
            signal_breakdown= result["signal_breakdown"],
        )
        message = format_decision_for_discord(decision)

        # Append score + stop price if available
        score = result.get("score")
        stop  = result.get("risk_detail", {}).get("stop_price")
        if score:
            message += f"\n**Score:** {score}/100"
        if stop:
            message += f" | **Stop:** ${stop:.2f}"

        try:
            resp = requests.post(webhook_url, json={"content": message}, timeout=10)
            return resp.status_code == 204
        except Exception:
            return False

    # ──────────────────────────────────────────────────────────────────────
    # PORTFOLIO VALUE UPDATE
    # ──────────────────────────────────────────────────────────────────────

    def update_portfolio_value(self, new_value: float):
        """Update portfolio value in risk manager (call after P&L changes)."""
        self.portfolio_value = new_value
        self.risk_manager.portfolio_value = new_value
