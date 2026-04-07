"""
ap_technical_agent.py — Angel Precision Technical Analysis Agent
================================================================
Adapted from virattt/ai-hedge-fund technicals.py (MIT License)

Runs 5 technical strategies on a price DataFrame and returns a
weighted consensus signal + confidence score.

Compatible with your existing scanner output — call this AFTER your
Strat pattern scanner identifies a setup to get a second confirmation layer.

Usage:
    from ap_intelligence.agents.ap_technical_agent import APTechnicalAgent

    agent = APTechnicalAgent()
    result = agent.analyze(ticker="AAPL", df=prices_df)
    print(result)
    # → {"signal": "bullish", "confidence": 78, "breakdown": {...}}
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class TechSignal:
    signal: str      # "bullish" | "bearish" | "neutral"
    confidence: float  # 0.0 – 1.0
    metrics: dict


class APTechnicalAgent:
    """
    5-strategy weighted technical ensemble.
    Weights tuned for swing + options trading (momentum-heavy).
    """

    STRATEGY_WEIGHTS = {
        "trend":          0.25,
        "momentum":       0.30,   # Higher weight — most predictive for 0DTE/swing
        "mean_reversion": 0.15,
        "volatility":     0.15,
        "strat_pattern":  0.15,   # Angel Precision's own Strat pattern signal
    }

    def analyze(self, ticker: str, df: pd.DataFrame, strat_signal: str = "neutral") -> dict:
        """
        Run all 5 strategies and return consensus.

        Parameters
        ----------
        ticker : str
        df : pd.DataFrame with columns: open, high, low, close, volume
        strat_signal : str — your scanner's signal ("bullish"|"bearish"|"neutral")
        """
        if df.empty or len(df) < 20:
            return {"signal": "neutral", "confidence": 0, "ticker": ticker, "breakdown": {}}

        trend      = self._trend(df)
        momentum   = self._momentum(df)
        mean_rev   = self._mean_reversion(df)
        volatility = self._volatility(df)
        strat      = self._strat_signal_wrap(strat_signal)

        combined = self._weighted_combine({
            "trend":          trend,
            "momentum":       momentum,
            "mean_reversion": mean_rev,
            "volatility":     volatility,
            "strat_pattern":  strat,
        })

        return {
            "ticker":     ticker,
            "signal":     combined["signal"],
            "confidence": round(combined["confidence"] * 100),
            "breakdown": {
                "trend":          {"signal": trend.signal,     "confidence": round(trend.confidence * 100),     "metrics": trend.metrics},
                "momentum":       {"signal": momentum.signal,  "confidence": round(momentum.confidence * 100),  "metrics": momentum.metrics},
                "mean_reversion": {"signal": mean_rev.signal,  "confidence": round(mean_rev.confidence * 100),  "metrics": mean_rev.metrics},
                "volatility":     {"signal": volatility.signal,"confidence": round(volatility.confidence * 100),"metrics": volatility.metrics},
                "strat_pattern":  {"signal": strat.signal,     "confidence": round(strat.confidence * 100),     "metrics": strat.metrics},
            }
        }

    # ─────────────────────────────────────────────
    # STRATEGY 1: TREND FOLLOWING
    # ─────────────────────────────────────────────
    def _trend(self, df: pd.DataFrame) -> TechSignal:
        """EMA stack + ADX."""
        close = df["close"]

        ema8  = close.ewm(span=8,  adjust=False).mean()
        ema21 = close.ewm(span=21, adjust=False).mean()
        ema55 = close.ewm(span=55, adjust=False).mean()

        adx = self._adx(df, 14)

        bull = (ema8.iloc[-1] > ema21.iloc[-1]) and (ema21.iloc[-1] > ema55.iloc[-1])
        bear = (ema8.iloc[-1] < ema21.iloc[-1]) and (ema21.iloc[-1] < ema55.iloc[-1])

        adx_strength = min(float(adx.iloc[-1]) / 40.0, 1.0) if not adx.empty else 0.5

        if bull:
            return TechSignal("bullish", 0.5 + adx_strength * 0.5,
                              {"ema8": round(ema8.iloc[-1],2), "ema21": round(ema21.iloc[-1],2),
                               "ema55": round(ema55.iloc[-1],2), "adx": round(float(adx.iloc[-1]),2) if not adx.empty else 0})
        elif bear:
            return TechSignal("bearish", 0.5 + adx_strength * 0.5,
                              {"ema8": round(ema8.iloc[-1],2), "ema21": round(ema21.iloc[-1],2),
                               "ema55": round(ema55.iloc[-1],2), "adx": round(float(adx.iloc[-1]),2) if not adx.empty else 0})
        else:
            return TechSignal("neutral", 0.3, {"ema8": round(ema8.iloc[-1],2), "ema21": round(ema21.iloc[-1],2)})

    # ─────────────────────────────────────────────
    # STRATEGY 2: MOMENTUM
    # ─────────────────────────────────────────────
    def _momentum(self, df: pd.DataFrame) -> TechSignal:
        """RSI + MACD + price rate of change."""
        close = df["close"]

        rsi  = self._rsi(close, 14).iloc[-1]
        macd, signal = self._macd(close)
        macd_cross = float(macd.iloc[-1]) - float(signal.iloc[-1])

        roc_10 = float((close.iloc[-1] / close.iloc[-11] - 1) * 100) if len(close) > 10 else 0.0

        bull_pts = 0
        bear_pts = 0

        if rsi > 55:   bull_pts += 1
        elif rsi < 45: bear_pts += 1

        if macd_cross > 0:  bull_pts += 1
        elif macd_cross < 0: bear_pts += 1

        if roc_10 > 2:   bull_pts += 1
        elif roc_10 < -2: bear_pts += 1

        total = 3
        if bull_pts > bear_pts:
            return TechSignal("bullish", bull_pts / total,
                              {"rsi": round(rsi,2), "macd_cross": round(macd_cross,4), "roc_10": round(roc_10,2)})
        elif bear_pts > bull_pts:
            return TechSignal("bearish", bear_pts / total,
                              {"rsi": round(rsi,2), "macd_cross": round(macd_cross,4), "roc_10": round(roc_10,2)})
        else:
            return TechSignal("neutral", 0.33,
                              {"rsi": round(rsi,2), "macd_cross": round(macd_cross,4), "roc_10": round(roc_10,2)})

    # ─────────────────────────────────────────────
    # STRATEGY 3: MEAN REVERSION
    # ─────────────────────────────────────────────
    def _mean_reversion(self, df: pd.DataFrame) -> TechSignal:
        """Bollinger Bands + Z-score."""
        close = df["close"]
        ma20  = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20
        z     = (close - ma20) / std20

        price = close.iloc[-1]
        z_now = float(z.iloc[-1]) if not np.isnan(z.iloc[-1]) else 0.0

        if price < float(lower.iloc[-1]):   # Oversold → potential bounce
            conf = min(abs(z_now) / 3.0, 1.0)
            return TechSignal("bullish", conf, {"z_score": round(z_now,2), "bb_position": "below_lower"})
        elif price > float(upper.iloc[-1]): # Overbought → potential fade
            conf = min(abs(z_now) / 3.0, 1.0)
            return TechSignal("bearish", conf, {"z_score": round(z_now,2), "bb_position": "above_upper"})
        else:
            return TechSignal("neutral", 0.3, {"z_score": round(z_now,2), "bb_position": "inside"})

    # ─────────────────────────────────────────────
    # STRATEGY 4: VOLATILITY ANALYSIS
    # ─────────────────────────────────────────────
    def _volatility(self, df: pd.DataFrame) -> TechSignal:
        """ATR expansion/contraction + volume surge."""
        atr14 = (df["high"] - df["low"]).rolling(14).mean()
        atr5  = (df["high"] - df["low"]).rolling(5).mean()

        atr_ratio = float(atr5.iloc[-1] / atr14.iloc[-1]) if float(atr14.iloc[-1]) > 0 else 1.0

        vol_ratio = 1.0
        if "volume" in df.columns:
            avg_vol = df["volume"].rolling(20).mean().iloc[-1]
            if avg_vol > 0:
                vol_ratio = float(df["volume"].iloc[-1] / avg_vol)

        # Expanding ATR + volume surge = directional move likely
        if atr_ratio > 1.2 and vol_ratio > 1.3:
            # Direction from last price change
            direction = "bullish" if df["close"].iloc[-1] > df["open"].iloc[-1] else "bearish"
            conf = min((atr_ratio - 1.0) * 0.5 + (vol_ratio - 1.0) * 0.2, 1.0)
            return TechSignal(direction, conf,
                              {"atr_ratio": round(atr_ratio,3), "vol_ratio": round(vol_ratio,2)})
        else:
            return TechSignal("neutral", 0.3,
                              {"atr_ratio": round(atr_ratio,3), "vol_ratio": round(vol_ratio,2)})

    # ─────────────────────────────────────────────
    # STRATEGY 5: STRAT PATTERN PASSTHROUGH
    # ─────────────────────────────────────────────
    def _strat_signal_wrap(self, strat_signal: str) -> TechSignal:
        """Wrap your scanner's Strat signal as a TechSignal at 80% confidence when valid."""
        if strat_signal == "bullish":
            return TechSignal("bullish", 0.80, {"source": "AP_Strat_Scanner", "signal": "bullish"})
        elif strat_signal == "bearish":
            return TechSignal("bearish", 0.80, {"source": "AP_Strat_Scanner", "signal": "bearish"})
        else:
            return TechSignal("neutral", 0.30, {"source": "AP_Strat_Scanner", "signal": "neutral"})

    # ─────────────────────────────────────────────
    # WEIGHTED COMBINER
    # ─────────────────────────────────────────────
    def _weighted_combine(self, strategies: dict[str, TechSignal]) -> dict:
        """Weighted vote across all strategies."""
        bull_score = 0.0
        bear_score = 0.0
        total_weight = 0.0

        for name, sig in strategies.items():
            w = self.STRATEGY_WEIGHTS.get(name, 0.2)
            total_weight += w
            if sig.signal == "bullish":
                bull_score += w * sig.confidence
            elif sig.signal == "bearish":
                bear_score += w * sig.confidence

        if total_weight == 0:
            return {"signal": "neutral", "confidence": 0.0}

        if bull_score > bear_score:
            return {"signal": "bullish", "confidence": bull_score / total_weight}
        elif bear_score > bull_score:
            return {"signal": "bearish", "confidence": bear_score / total_weight}
        else:
            return {"signal": "neutral", "confidence": 0.3}

    # ─────────────────────────────────────────────
    # INDICATOR HELPERS
    # ─────────────────────────────────────────────
    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        rs    = gain / loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _macd(close: pd.Series, fast=12, slow=26, signal=9):
        ema_fast   = close.ewm(span=fast, adjust=False).mean()
        ema_slow   = close.ewm(span=slow, adjust=False).mean()
        macd_line  = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        return macd_line, signal_line

    @staticmethod
    def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        tr    = pd.concat([high - low,
                           (high - close.shift()).abs(),
                           (low  - close.shift()).abs()], axis=1).max(axis=1)
        dm_plus  = (high - high.shift()).clip(lower=0)
        dm_minus = (low.shift() - low).clip(lower=0)
        atr      = tr.rolling(period).mean()
        di_plus  = 100 * dm_plus.rolling(period).mean() / atr.replace(0, np.nan)
        di_minus = 100 * dm_minus.rolling(period).mean() / atr.replace(0, np.nan)
        dx       = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
        return dx.rolling(period).mean()
