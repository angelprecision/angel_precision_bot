"""
ap_sentiment_agent.py — Angel Precision Sentiment Agent
=========================================================
Adapted from virattt/ai-hedge-fund (sentiment.py + news_sentiment.py) MIT License
+ Rallies CLI prompt engineering patterns

Combines:
1. News sentiment (yfinance headlines → GPT-4 classification)
2. Insider trade direction (yfinance)
3. Short interest ratio (high short = bearish pressure)
4. Optional Reddit/social mention spike detection

Returns: signal ("bullish"|"bearish"|"neutral"), confidence (0-100), reasoning dict

Usage:
    from ap_intelligence.agents.ap_sentiment_agent import APSentimentAgent
    agent = APSentimentAgent(openai_api_key="sk-...")
    result = agent.analyze("AAPL")
"""

import os
import json
import numpy as np
import pandas as pd
from ap_intelligence.tools.ap_data_tools import get_company_news, get_insider_trades, get_financial_metrics


class APSentimentAgent:
    """
    Multi-source sentiment analyst for Angel Precision.
    Uses LLM classification for news + rule-based insider/short interest signals.
    """

    INSIDER_WEIGHT = 0.25
    NEWS_WEIGHT    = 0.55
    SHORT_WEIGHT   = 0.20

    def __init__(self, openai_api_key: str = None):
        self.openai_api_key = openai_api_key or os.environ.get("OPENAI_API_KEY", "")

    def analyze(self, ticker: str, use_llm: bool = True) -> dict:
        """
        Full sentiment analysis for a ticker.
        Returns dict: {signal, confidence, reasoning}
        """
        # ── 1. News Sentiment ──────────────────────────────────
        news_items = get_company_news(ticker, limit=20)
        news_signals, news_reasoning = self._analyze_news(ticker, news_items, use_llm)

        # ── 2. Insider Trades ──────────────────────────────────
        insider_df = get_insider_trades(ticker)
        insider_signal, insider_reasoning = self._analyze_insider(insider_df)

        # ── 3. Short Interest ──────────────────────────────────
        metrics = get_financial_metrics(ticker)
        short_signal, short_reasoning = self._analyze_short_interest(metrics)

        # ── 4. Weighted Combine ────────────────────────────────
        def score(signals: list) -> float:
            """Convert signal list to -1 → +1 score."""
            map_ = {"bullish": 1, "neutral": 0, "bearish": -1}
            vals = [map_.get(s, 0) for s in signals]
            return float(np.mean(vals)) if vals else 0.0

        news_score   = score(news_signals)
        insider_score = {"bullish": 1, "neutral": 0, "bearish": -1}.get(insider_signal, 0)
        short_score  = {"bullish": 1, "neutral": 0, "bearish": -1}.get(short_signal, 0)

        combined = (
            news_score   * self.NEWS_WEIGHT +
            insider_score * self.INSIDER_WEIGHT +
            short_score  * self.SHORT_WEIGHT
        )

        if combined > 0.1:
            overall_signal = "bullish"
            confidence = round(min(combined / 1.0 * 100, 100), 1)
        elif combined < -0.1:
            overall_signal = "bearish"
            confidence = round(min(abs(combined) / 1.0 * 100, 100), 1)
        else:
            overall_signal = "neutral"
            confidence = round((1 - abs(combined)) * 50, 1)

        return {
            "ticker":     ticker,
            "signal":     overall_signal,
            "confidence": confidence,
            "reasoning": {
                "news_sentiment":   news_reasoning,
                "insider_trading":  insider_reasoning,
                "short_interest":   short_reasoning,
                "combined_score":   round(combined, 4),
            }
        }

    # ─────────────────────────────────────────────
    # NEWS ANALYSIS
    # ─────────────────────────────────────────────
    def _analyze_news(self, ticker: str, news_items: list, use_llm: bool) -> tuple[list, dict]:
        """Classify news headlines as bullish/bearish/neutral."""
        if not news_items:
            return [], {"total_articles": 0, "signal": "neutral", "confidence": 0}

        signals = []
        llm_classified = 0

        for item in news_items[:15]:  # Analyze max 15 headlines
            sentiment = item.get("sentiment")
            if sentiment is None and use_llm and self.openai_api_key:
                sentiment = self._llm_classify_headline(ticker, item.get("title", ""))
                item["sentiment"] = sentiment
                llm_classified += 1
            elif sentiment is None:
                sentiment = "neutral"

            if sentiment == "positive":
                signals.append("bullish")
            elif sentiment == "negative":
                signals.append("bearish")
            else:
                signals.append("neutral")

        bullish = signals.count("bullish")
        bearish = signals.count("bearish")
        neutral = signals.count("neutral")
        total   = len(signals)

        if bullish > bearish:
            sig  = "bullish"
            conf = round(bullish / total * 100, 1)
        elif bearish > bullish:
            sig  = "bearish"
            conf = round(bearish / total * 100, 1)
        else:
            sig  = "neutral"
            conf = 50.0

        reasoning = {
            "signal":          sig,
            "confidence":      conf,
            "total_articles":  total,
            "bullish":         bullish,
            "bearish":         bearish,
            "neutral":         neutral,
            "llm_classified":  llm_classified,
            "headlines":       [n["title"] for n in news_items[:5]],
        }
        return signals, reasoning

    def _llm_classify_headline(self, ticker: str, headline: str) -> str:
        """
        Use GPT-4 to classify a headline as positive/negative/neutral.
        Adapted from virattt/ai-hedge-fund news_sentiment_agent + Rallies prompt style.
        """
        try:
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)

            prompt = (
                f"Classify the sentiment of this news headline for stock {ticker}. "
                f"Answer with exactly one word: positive, negative, or neutral.\n\n"
                f"Headline: {headline}"
            )
            resp = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5,
                temperature=0,
            )
            word = resp.choices[0].message.content.strip().lower()
            if word in ("positive", "negative", "neutral"):
                return word
            return "neutral"
        except Exception:
            return "neutral"

    # ─────────────────────────────────────────────
    # INSIDER TRADES
    # ─────────────────────────────────────────────
    def _analyze_insider(self, df: pd.DataFrame) -> tuple[str, dict]:
        """Net insider buying vs selling over last 90 days."""
        if df is None or df.empty:
            return "neutral", {"signal": "neutral", "reason": "No insider data"}

        # yfinance column names may vary
        shares_col = None
        for col in ["Shares", "shares", "transaction_shares", "Value"]:
            if col in df.columns:
                shares_col = col
                break

        if shares_col is None:
            return "neutral", {"signal": "neutral", "reason": "No shares column found"}

        vals = pd.to_numeric(df[shares_col], errors="coerce").dropna()
        net  = float(vals.sum())

        if net > 0:
            signal = "bullish"
        elif net < 0:
            signal = "bearish"
        else:
            signal = "neutral"

        return signal, {
            "signal":       signal,
            "net_shares":   round(net, 0),
            "total_trades": len(vals),
            "buy_trades":   int((vals > 0).sum()),
            "sell_trades":  int((vals < 0).sum()),
        }

    # ─────────────────────────────────────────────
    # SHORT INTEREST
    # ─────────────────────────────────────────────
    def _analyze_short_interest(self, metrics: dict) -> tuple[str, dict]:
        """High short ratio = bearish pressure (squeeze risk = bullish)."""
        short_ratio = metrics.get("short_ratio")
        if short_ratio is None:
            return "neutral", {"signal": "neutral", "reason": "No short ratio data"}

        short_ratio = float(short_ratio)

        # Short ratio interpretation:
        # <2 = low short, <5 = moderate, >10 = very high (possible squeeze)
        if short_ratio > 10:
            # Extreme short — potential squeeze = slightly bullish
            signal = "bullish"
            reason = f"Short ratio {short_ratio:.1f} — squeeze risk"
        elif short_ratio > 5:
            signal = "bearish"
            reason = f"Short ratio {short_ratio:.1f} — elevated short interest"
        else:
            signal = "neutral"
            reason = f"Short ratio {short_ratio:.1f} — normal"

        return signal, {"signal": signal, "short_ratio": short_ratio, "reason": reason}
