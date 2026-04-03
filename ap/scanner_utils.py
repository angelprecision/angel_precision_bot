# ap_scanner_utils.py — Angel Precision Shared Scanner Utilities
# ================================================================
# Drop this file in your repo root (same level as your scanners).
#
# Import in each scanner:
#   from ap_scanner_utils import (
#       classify_candle, is_inside,
#       atr14, atr_stop, price_targets_atr,
#       find_nearest_wick_up, find_nearest_wick_down,
#       nearest_strike, pick_expiry, volume_ratio,
#       TICKERS, dedupe_keep_order,
#   )
#
# BUGS THIS FIXES IN EVERY SCANNER:
#   ❌ get_expiry_label() → always returned "DAILY" (2-3, 3-1-2 scanners)
#   ❌ find_price_targets() → hardcoded $1/$2/$3.50 dollar ranges
#       (breaks on high-price stocks like NVDA, MSFT, AAPL)
#       Replaced by price_targets_atr() using 0.5/1.0/1.5x ATR — scales with stock price
#   ❌ atr_stop() → stop = midpoint of bar (ignores volatility)
#       Replaced by atr_stop() using 0.5x 14-period ATR from entry
#   ❌ Duplicate code across 4 scanners — one fix here fixes all
#   ❌ Volume confirmation missing — volume_ratio() added
# ================================================================

import numpy as np

# ── TICKER UNIVERSE (single source of truth) ──────────────────────────────

INDICES = ["SPY", "QQQ", "IWM", "^GSPC"]

SP100 = [
    "AAPL", "ABBV", "ABT", "ACN", "ADBE", "AIG", "AMD", "AMGN", "AMT", "AMZN",
    "AVGO", "AXP", "BA", "BAC", "BK", "BKNG", "BLK", "BMY", "BRK.B", "C",
    "CAT", "CHTR", "CL", "CMCSA", "COF", "COP", "COST", "CRM", "CSCO", "CVS",
    "CVX", "DE", "DHR", "DIS", "DOW", "DUK", "EMR", "EXC", "F", "FDX",
    "GD", "GE", "GILD", "GM", "GOOGL", "GS", "HD", "HON", "IBM", "INTC",
    "INTU", "ISRG", "JNJ", "JPM", "KO", "LIN", "LLY", "LMT", "LOW", "MA",
    "MCD", "MDLZ", "MDT", "MET", "META", "MMM", "MO", "MRK", "MS", "MSFT",
    "NEE", "NFLX", "NKE", "NVDA", "ORCL", "PEP", "PFE", "PG", "PM", "PYPL",
    "QCOM", "RTX", "SBUX", "SCHW", "SO", "SPG", "T", "TGT", "TMO", "TSLA",
    "TXN", "UNH", "UNP", "UPS", "USB", "V", "VZ", "WFC", "WMT", "XOM",
]

HEAVYWEIGHTS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "AVGO", "COST", "AMD", "NFLX", "ADBE", "QCOM", "TXN",
]

REST_NASDAQ = [
    "ASML", "PEP", "CSCO", "TMUS", "CMCSA", "INTC",
    "INTU", "AMGN", "HON", "AMAT", "SBUX", "BKNG", "ISRG", "PANW", "ADP", "GILD",
    "ADI", "VRTX", "MDLZ", "REGN", "LRCX", "MU", "PYPL", "SNPS", "CDNS", "KLAC",
    "MELI", "CRWD", "MAR", "CSX", "ORLY", "FTNT", "ADSK", "ABNB", "DASH", "NXPI",
    "WDAY", "CTAS", "ROP", "PCAR", "MNST", "CPRT", "AEP", "PAYX", "CHTR", "MCHP",
    "FAST", "ROST", "ODFL", "KDP", "EA", "GEHC", "BKR", "VRSK", "EXC", "CTSH",
    "DXCM", "KHC", "CCEP", "XEL", "LULU", "IDXX", "TEAM", "ON", "ANSS", "FANG",
    "TTWO", "CSGP", "ZS", "DDOG", "BIIB", "ILMN", "MDB", "WBD", "GFS", "MRNA",
    "CDW", "ARM", "SMCI", "DLTR", "WBA", "PDD", "ZM", "LCID", "RIVN", "SIRI",
]

EXTRA_TICKERS = ["DELL", "ORCL", "CRM", "IBM", "NOW", "HOOD", "V", "MA"]

# Tickers that support 0DTE options
_ZERO_DTE = {"SPY", "QQQ", "IWM", "SPX", "NDX"}


def dedupe_keep_order(items: list) -> list:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


TICKERS = dedupe_keep_order(INDICES + SP100 + HEAVYWEIGHTS + REST_NASDAQ + EXTRA_TICKERS)


# ── CANDLE CLASSIFIER ─────────────────────────────────────────────────────

def classify_candle(curr, prev) -> int:
    """
    Standard Strat candle type:
      1 = inside bar  (current fully inside previous)
      2 = directional (higher high OR lower low, not both)
      3 = outside bar (higher high AND lower low)
    """
    ch, cl = float(curr["High"]), float(curr["Low"])
    ph, pl = float(prev["High"]), float(prev["Low"])
    if ch < ph and cl > pl:
        return 1
    if ch > ph and cl < pl:
        return 3
    return 2


def is_inside(curr, prev) -> bool:
    """True if curr is fully inside prev (1-bar). Used by consolidation scanner."""
    return (
        float(curr["High"]) <= float(prev["High"]) and
        float(curr["Low"])  >= float(prev["Low"])
    )


def is_2U(curr, prev) -> bool:
    """Directional UP: higher high, no lower low."""
    return (
        float(curr["High"]) > float(prev["High"]) and
        float(curr["Low"])  >= float(prev["Low"])
    )


def is_2D(curr, prev) -> bool:
    """Directional DOWN: lower low, no higher high."""
    return (
        float(curr["Low"])   < float(prev["Low"]) and
        float(curr["High"]) <= float(prev["High"])
    )


# ── ATR ───────────────────────────────────────────────────────────────────

def atr14(df) -> float:
    """
    14-period Average True Range. Falls back gracefully if < 14 bars.
    """
    highs  = df["High"].values.astype(float)
    lows   = df["Low"].values.astype(float)
    closes = df["Close"].values.astype(float)
    trs = [
        max(
            highs[i] - lows[i],
            abs(highs[i]  - closes[i - 1]),
            abs(lows[i]   - closes[i - 1]),
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
    Replaces the old midpoint-of-bar stop (which didn't scale with price).

    direction: "UP"   → stop is below entry (CALL protection)
               "DOWN" → stop is above entry (PUT  protection)
    """
    atr  = atr14(df)
    sign = -1 if direction == "UP" else 1
    return round(entry + sign * atr * 0.5, 2)


# ── ATR-BASED PRICE TARGETS ───────────────────────────────────────────────

def price_targets_atr(df, entry: float, direction: str) -> dict:
    """
    ATR-scaled price targets. Replaces hardcoded $1/$2/$3.50 bands
    that always returned None for NVDA ($900), MSFT ($420), etc.

      PT1 = 0.5× ATR from entry
      PT2 = 1.0× ATR from entry
      PT3 = 1.5× ATR from entry

    direction: "UP" → targets above entry (calls)
               "DOWN" → targets below entry (puts)
    """
    atr  = atr14(df)
    sign = 1 if direction == "UP" else -1
    return {
        "PT1": round(entry + sign * atr * 0.5, 2),
        "PT2": round(entry + sign * atr * 1.0, 2),
        "PT3": round(entry + sign * atr * 1.5, 2),
    }


# ── WICK-BASED SINGLE TARGET (used by consolidation scanner) ──────────────

def find_nearest_wick_up(df, reference_high: float, lookback: int = 80) -> float | None:
    """
    Nearest upper wick above reference_high (single target for 1-1 scanner).
    Returns None if no suitable wick found in lookback bars.
    """
    df = df.dropna()
    start = max(0, len(df) - lookback - 1)
    for i in range(len(df) - 2, start - 1, -1):
        c = df.iloc[i]
        body_top  = max(float(c["Open"]), float(c["Close"]))
        upper_wick = float(c["High"]) - body_top
        if upper_wick > 0 and float(c["High"]) > float(reference_high):
            return float(c["High"])
    return None


def find_nearest_wick_down(df, reference_low: float, lookback: int = 80) -> float | None:
    """
    Nearest lower wick below reference_low (single target for 1-1 scanner).
    Returns None if no suitable wick found in lookback bars.
    """
    df = df.dropna()
    start = max(0, len(df) - lookback - 1)
    for i in range(len(df) - 2, start - 1, -1):
        c = df.iloc[i]
        body_bottom = min(float(c["Open"]), float(c["Close"]))
        lower_wick  = body_bottom - float(c["Low"])
        if lower_wick > 0 and float(c["Low"]) < float(reference_low):
            return float(c["Low"])
    return None


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

def pick_expiry(ticker: str) -> str:
    """
    FIX: old scanners returned "DAILY" for every ticker — that's wrong.
    0DTE is only valid for SPY, QQQ, IWM, SPX, NDX.
    Everything else should be WEEKLY.

      Indices → "0DTE"
      Stocks  → "WEEKLY"
    """
    return "0DTE" if ticker.upper() in _ZERO_DTE else "WEEKLY"


# ── VOLUME CONFIRMATION ───────────────────────────────────────────────────

def volume_ratio(df) -> float:
    """
    Current bar volume / 20-bar average volume.
    1.0 = average, 1.5 = 50% above average.
    Returns 1.0 on error so callers don't crash.
    """
    try:
        avg  = float(df["Volume"].iloc[-20:].mean())
        last = float(df["Volume"].iloc[-1])
        return round(last / avg, 2) if avg > 0 else 1.0
    except Exception:
        return 1.0
