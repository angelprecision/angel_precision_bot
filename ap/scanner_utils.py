# ap_scanner_utils.py — Angel Precision Shared Scanner Utilities
# ================================================================
# Drop this file in your repo root (same level as your scanners).
# Import it in each individual scanner:
#
#   from ap_scanner_utils import (
#       classify, is_2U, is_2D,
#       atr14, atr_stop, price_targets,
#       nearest_strike, pick_expiry, volume_ratio
#   )
#
# WHAT THIS REPLACES IN YOUR EXISTING SCANNERS:
#   ❌ Hardcoded stops (e.g. midpoint of outside bar)
#   ❌ Hardcoded PT1/PT2/PT3 ($1, $2, $3.50)
#   ❌ pick_expiry() returning "DAILY" for everything
#   ❌ Missing volume confirmation
# ================================================================

import numpy as np


# ── CANDLE CLASSIFIER ─────────────────────────────────────────────────────

def classify(curr, prev) -> int:
    """
    Returns Strat candle type:
      1 = inside bar  (current inside previous)
      2 = directional (higher high OR lower low, not both)
      3 = outside bar (higher high AND lower low)
    """
    ch, cl = float(curr["High"]), float(curr["Low"])
    ph, pl = float(prev["High"]), float(prev["Low"])
    if ch < ph and cl > pl:
        return 1  # inside
    if ch > ph and cl < pl:
        return 3  # outside
    return 2       # directional


def is_2U(curr, prev) -> bool:
    """Directional UP: higher high, no lower low."""
    return (
        float(curr["High"]) > float(prev["High"]) and
        float(curr["Low"]) >= float(prev["Low"])
    )


def is_2D(curr, prev) -> bool:
    """Directional DOWN: lower low, no higher high."""
    return (
        float(curr["Low"]) < float(prev["Low"]) and
        float(curr["High"]) <= float(prev["High"])
    )


# ── ATR ───────────────────────────────────────────────────────────────────

def atr14(df) -> float:
    """
    14-period Average True Range.
    Falls back to available bars if fewer than 14.
    """
    highs  = df["High"].values
    lows   = df["Low"].values
    closes = df["Close"].values
    trs = [
        max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        for i in range(1, len(highs))
    ]
    if not trs:
        return 1.0
    return float(np.mean(trs[-14:])) if len(trs) >= 14 else float(np.mean(trs))


# ── ATR-BASED STOP LOSS ───────────────────────────────────────────────────

def atr_stop(df, entry: float, direction: str) -> float:
    """
    Stop loss = 0.5x ATR from entry.
    Replaces hardcoded midpoint-of-bar stops.

    direction: "UP" (long/call) or "DOWN" (short/put)
    """
    atr  = atr14(df)
    sign = -1 if direction == "UP" else 1
    return round(entry + sign * atr * 0.5, 2)


# ── ATR-BASED PRICE TARGETS ───────────────────────────────────────────────

def price_targets(df, entry: float, direction: str) -> dict:
    """
    PT1 = 0.5x ATR from entry
    PT2 = 1.0x ATR from entry
    PT3 = 1.5x ATR from entry

    Replaces hardcoded $1 / $2 / $3.50 targets.

    direction: "UP" (call) or "DOWN" (put)
    """
    atr  = atr14(df)
    sign = 1 if direction == "UP" else -1
    return {
        "PT1": round(entry + sign * atr * 0.5, 2),
        "PT2": round(entry + sign * atr * 1.0, 2),
        "PT3": round(entry + sign * atr * 1.5, 2),
    }


# ── STRIKE SELECTION ──────────────────────────────────────────────────────

def nearest_strike(price: float) -> float:
    """
    Round to nearest standard strike increment based on price range.
      < $25   → $0.50 increments
      < $100  → $1.00 increments
      < $200  → $2.50 increments
      $200+   → $5.00 increments
    """
    if price < 25:
        return round(price * 2) / 2
    if price < 100:
        return round(price)
    if price < 200:
        return round(price / 2.5) * 2.5
    return round(price / 5) * 5


# ── EXPIRY SELECTION ──────────────────────────────────────────────────────

# Tickers that support 0DTE (daily options)
_ZERO_DTE_TICKERS = {"SPY", "QQQ", "IWM", "SPX", "NDX"}

def pick_expiry(ticker: str) -> str:
    """
    Returns the recommended expiry hint for a ticker.
    Fixes the old bug where this returned "DAILY" for everything.

      Indices (SPY, QQQ, IWM, SPX, NDX) → "0DTE"
      Everything else                    → "WEEKLY"
    """
    return "0DTE" if ticker.upper() in _ZERO_DTE_TICKERS else "WEEKLY"


# ── VOLUME CONFIRMATION ───────────────────────────────────────────────────

def volume_ratio(df) -> float:
    """
    Current bar volume vs 20-bar average.
    Returns ratio (1.5 = 50% above average).
    Signals with ratio < 1.0 are below-average volume — treat with caution.
    """
    try:
        avg  = float(df["Volume"].iloc[-20:].mean())
        last = float(df["Volume"].iloc[-1])
        return round(last / avg, 2) if avg > 0 else 1.0
    except Exception:
        return 1.0
