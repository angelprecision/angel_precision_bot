"""
ap_fundamentals_agent.py — Angel Precision Fundamentals Agent
=============================================================
Adapted from virattt/ai-hedge-fund fundamentals.py (MIT License)

Scores a stock on 4 fundamental dimensions:
1. Profitability  (ROE, Net Margin, Operating Margin)
2. Growth         (Revenue, Earnings, Book Value growth)
3. Financial Health (Current Ratio, D/E, FCF conversion)
4. Valuation       (P/E, P/B, P/S)

For swing trade filtering — use to avoid entering a technically
good pattern on a fundamentally broken company.

Usage:
    from ap_intelligence.agents.ap_fundamentals_agent import APFundamentalsAgent
    agent = APFundamentalsAgent()
    result = agent.analyze("NVDA")
    print(result["signal"], result["confidence"])
"""

from ap_intelligence.tools.ap_data_tools import get_financial_metrics


class APFundamentalsAgent:
    """
    Score a stock on fundamentals. Used as a filter layer on top of
    scanner signals — blocks trades on structurally weak companies.
    """

    def analyze(self, ticker: str) -> dict:
        metrics = get_financial_metrics(ticker)
        if not metrics:
            return {"ticker": ticker, "signal": "neutral", "confidence": 0,
                    "reasoning": {"error": "No fundamental data available"}}

        signals   = []
        reasoning = {}

        # ── 1. Profitability ──────────────────────────────────
        roe       = metrics.get("roe")
        net_mgn   = metrics.get("net_margin")
        op_mgn    = metrics.get("operating_margin")

        prof_score = sum([
            roe     is not None and roe     > 0.15,   # ROE > 15%
            net_mgn is not None and net_mgn > 0.20,   # Net margin > 20%
            op_mgn  is not None and op_mgn  > 0.15,   # Op margin > 15%
        ])
        prof_signal = "bullish" if prof_score >= 2 else "bearish" if prof_score == 0 else "neutral"
        signals.append(prof_signal)
        reasoning["profitability"] = {
            "signal":          prof_signal,
            "roe":             f"{roe:.1%}" if roe else "N/A",
            "net_margin":      f"{net_mgn:.1%}" if net_mgn else "N/A",
            "operating_margin":f"{op_mgn:.1%}" if op_mgn else "N/A",
            "score":           f"{prof_score}/3",
        }

        # ── 2. Growth ─────────────────────────────────────────
        rev_growth = metrics.get("revenue_growth")
        earn_growth = metrics.get("earnings_growth")

        grow_score = sum([
            rev_growth  is not None and rev_growth  > 0.10,   # >10% revenue growth
            earn_growth is not None and earn_growth > 0.10,   # >10% earnings growth
        ])
        grow_signal = "bullish" if grow_score >= 2 else "bearish" if grow_score == 0 else "neutral"
        signals.append(grow_signal)
        reasoning["growth"] = {
            "signal":          grow_signal,
            "revenue_growth":  f"{rev_growth:.1%}" if rev_growth else "N/A",
            "earnings_growth": f"{earn_growth:.1%}" if earn_growth else "N/A",
            "score":           f"{grow_score}/2",
        }

        # ── 3. Financial Health ───────────────────────────────
        current_ratio = metrics.get("current_ratio")
        dte           = metrics.get("debt_to_equity")

        health_score = sum([
            current_ratio is not None and current_ratio > 1.5,      # Strong liquidity
            dte           is not None and dte           < 1.0,       # Manageable debt
        ])
        health_signal = "bullish" if health_score >= 2 else "bearish" if health_score == 0 else "neutral"
        signals.append(health_signal)
        reasoning["financial_health"] = {
            "signal":        health_signal,
            "current_ratio": f"{current_ratio:.2f}" if current_ratio else "N/A",
            "debt_to_equity":f"{dte:.2f}" if dte else "N/A",
            "score":         f"{health_score}/2",
        }

        # ── 4. Valuation ──────────────────────────────────────
        pe = metrics.get("pe_ratio")
        pb = metrics.get("pb_ratio")
        ps = metrics.get("ps_ratio")

        # High valuation = bearish signal (overvalued)
        overval_score = sum([
            pe is not None and pe > 40,   # Very high P/E
            pb is not None and pb > 5,    # Very high P/B
            ps is not None and ps > 10,   # Very high P/S
        ])
        val_signal = "bearish" if overval_score >= 2 else "bullish" if overval_score == 0 else "neutral"
        signals.append(val_signal)
        reasoning["valuation"] = {
            "signal":   val_signal,
            "pe_ratio": f"{pe:.1f}" if pe else "N/A",
            "pb_ratio": f"{pb:.1f}" if pb else "N/A",
            "ps_ratio": f"{ps:.1f}" if ps else "N/A",
            "note":     "bearish = overvalued; bullish = fairly/undervalued",
            "score":    f"{overval_score} overvalued metrics out of 3",
        }

        # ── 5. Overall Signal ─────────────────────────────────
        bull = signals.count("bullish")
        bear = signals.count("bearish")

        if bull > bear:
            overall = "bullish"
        elif bear > bull:
            overall = "bearish"
        else:
            overall = "neutral"

        confidence = round(max(bull, bear) / len(signals) * 100)

        return {
            "ticker":     ticker,
            "signal":     overall,
            "confidence": confidence,
            "reasoning":  reasoning,
            "raw_metrics": {
                "beta":       metrics.get("beta"),
                "market_cap": metrics.get("market_cap"),
                "sector":     metrics.get("sector"),
                "industry":   metrics.get("industry"),
                "52w_high":   metrics.get("52w_high"),
                "52w_low":    metrics.get("52w_low"),
            }
        }
