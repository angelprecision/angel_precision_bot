"""
ap_data_tools.py — Angel Precision Data Layer
=============================================
Adapted from virattt/ai-hedge-fund (MIT License)
Extended for Tradier + yfinance (no FinancialDatasets.ai required)

Provides:
- Price data (Tradier primary, yfinance fallback)
- Financial metrics via yfinance
- News sentiment via yfinance + NewsAPI
- Insider trades via yfinance
- Caching layer to avoid redundant API calls
"""

import logging
import os
import json
import time
import datetime
import requests
import pandas as pd
import numpy as np
from pathlib import Path

log = logging.getLogger("ap.data_tools")

# ─────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────
CACHE_DIR = Path(os.environ.get("AP_CACHE_DIR", "/tmp/ap_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(key: str) -> Path:
    safe = key.replace("/", "_").replace(":", "_")
    return CACHE_DIR / f"{safe}.json"

def _cache_get(key: str):
    p = _cache_path(key)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < 3600:  # 1 hour TTL
            with open(p) as f:
                return json.load(f)
    return None

def _cache_set(key: str, data):
    p = _cache_path(key)
    with open(p, "w") as f:
        json.dump(data, f)

# ─────────────────────────────────────────────
# TRADIER PRICE DATA
# ─────────────────────────────────────────────
TRADIER_TOKEN = os.environ.get("TRADIER_ACCESS_TOKEN", "")
TRADIER_BASE  = "https://api.tradier.com/v1/markets"

def get_prices_tradier(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch OHLCV history from Tradier."""
    cache_key = f"tradier_prices_{ticker}_{start_date}_{end_date}"
    cached = _cache_get(cache_key)
    if cached:
        return pd.DataFrame(cached)

    headers = {
        "Authorization": f"Bearer {TRADIER_TOKEN}",
        "Accept": "application/json",
    }
    params = {
        "symbol": ticker,
        "interval": "daily",
        "start": start_date,
        "end": end_date,
        "session_filter": "open",
    }
    resp = requests.get(f"{TRADIER_BASE}/history", headers=headers, params=params, timeout=15)
    if resp.status_code != 200:
        return pd.DataFrame()

    data = resp.json().get("history", {}) or {}
    days = data.get("day", [])
    if not days:
        return pd.DataFrame()
    if isinstance(days, dict):
        days = [days]

    df = pd.DataFrame(days)
    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={"date": "Date", "open": "open", "high": "high",
                             "low": "low", "close": "close", "volume": "volume"})
    df = df.set_index("Date").sort_index()
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    _cache_set(cache_key, df.reset_index().to_dict(orient="records"))
    return df


def get_prices_yfinance(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fallback: fetch OHLCV from yfinance."""
    try:
        import yfinance as yf
        cache_key = f"yf_prices_{ticker}_{start_date}_{end_date}"
        cached = _cache_get(cache_key)
        if cached:
            return pd.DataFrame(cached)

        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True, progress=False)
        if df.empty:
            return pd.DataFrame()
        df.columns = [c.lower() for c in df.columns]
        df.index.name = "Date"
        _cache_set(cache_key, df.reset_index().to_dict(orient="records"))
        return df
    except Exception as e:
        log.warning("get_prices_yfinance(%s) failed: %s", ticker, e)  # MED-009
        return pd.DataFrame()


def get_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Main entry: Tradier first, yfinance fallback."""
    df = get_prices_tradier(ticker, start_date, end_date)
    if df.empty:
        df = get_prices_yfinance(ticker, start_date, end_date)
    return df


# ─────────────────────────────────────────────
# FINANCIAL METRICS (yfinance)
# ─────────────────────────────────────────────
def get_financial_metrics(ticker: str) -> dict:
    """
    Pull key fundamentals from yfinance.
    Returns dict with: pe_ratio, pb_ratio, roe, net_margin,
    revenue_growth, debt_to_equity, current_ratio, market_cap
    """
    cache_key = f"yf_metrics_{ticker}_{datetime.date.today()}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        metrics = {
            "pe_ratio":           info.get("trailingPE"),
            "pb_ratio":           info.get("priceToBook"),
            "ps_ratio":           info.get("priceToSalesTrailing12Months"),
            "roe":                info.get("returnOnEquity"),
            "net_margin":         info.get("profitMargins"),
            "operating_margin":   info.get("operatingMargins"),
            "revenue_growth":     info.get("revenueGrowth"),
            "earnings_growth":    info.get("earningsGrowth"),
            "debt_to_equity":     info.get("debtToEquity"),
            "current_ratio":      info.get("currentRatio"),
            "free_cashflow":      info.get("freeCashflow"),
            "market_cap":         info.get("marketCap"),
            "beta":               info.get("beta"),
            "52w_high":           info.get("fiftyTwoWeekHigh"),
            "52w_low":            info.get("fiftyTwoWeekLow"),
            "short_ratio":        info.get("shortRatio"),
            "sector":             info.get("sector"),
            "industry":           info.get("industry"),
        }
        _cache_set(cache_key, metrics)
        return metrics
    except Exception as e:
        log.warning("get_financial_metrics(%s) failed: %s", ticker, e)  # MED-009
        return {}


# ─────────────────────────────────────────────
# NEWS SENTIMENT
# ─────────────────────────────────────────────
def get_company_news(ticker: str, limit: int = 20) -> list[dict]:
    """
    Pull recent news for ticker via yfinance.
    Returns list of {title, publisher, link, published}.
    """
    cache_key = f"yf_news_{ticker}_{datetime.date.today()}"
    cached = _cache_get(cache_key)
    if cached:
        return cached[:limit]

    try:
        import yfinance as yf
        news = yf.Ticker(ticker).news or []
        results = []
        for item in news:
            results.append({
                "title":     item.get("title", ""),
                "publisher": item.get("publisher", ""),
                "link":      item.get("link", ""),
                "published": item.get("providerPublishTime", 0),
                "sentiment": None,  # will be filled by LLM agent
            })
        _cache_set(cache_key, results)
        return results[:limit]
    except Exception as e:
        log.warning("get_company_news(%s) failed: %s", ticker, e)  # MED-009
        return []


# ─────────────────────────────────────────────
# INSIDER TRADES (yfinance)
# ─────────────────────────────────────────────
def get_insider_trades(ticker: str) -> pd.DataFrame:
    """Pull insider transactions from yfinance."""
    cache_key = f"yf_insider_{ticker}_{datetime.date.today()}"
    cached = _cache_get(cache_key)
    if cached:
        return pd.DataFrame(cached)

    try:
        import yfinance as yf
        df = yf.Ticker(ticker).insider_transactions
        if df is None or df.empty:
            return pd.DataFrame()
        _cache_set(cache_key, df.to_dict(orient="records"))
        return df
    except Exception as e:
        log.warning("get_insider_trades(%s) failed: %s", ticker, e)  # MED-009
        return pd.DataFrame()


# ─────────────────────────────────────────────
# MARKET CONTEXT
# ─────────────────────────────────────────────
def get_spy_trend(lookback: int = 20) -> dict:
    """SPY trend gate — used to qualify CALL vs PUT signals."""
    end = datetime.date.today().strftime("%Y-%m-%d")
    start = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
    df = get_prices("SPY", start, end)
    if df.empty or len(df) < lookback:
        return {"trend": "UNKNOWN", "above_ma": None, "ma": None, "price": None}

    ma = df["close"].rolling(lookback).mean().iloc[-1]
    price = df["close"].iloc[-1]
    above = price > ma

    # ATR-based condition
    atr = (df["high"] - df["low"]).rolling(14).mean().iloc[-1]
    slope = (df["close"].iloc[-1] - df["close"].iloc[-lookback]) / lookback

    if above and slope > 0:
        trend = "BULL"
    elif not above and slope < 0:
        trend = "BEAR"
    else:
        trend = "CHOPPY"

    return {
        "trend":    trend,
        "above_ma": bool(above),
        "ma":       round(float(ma), 2),
        "price":    round(float(price), 2),
        "atr":      round(float(atr), 2),
        "slope":    round(float(slope), 4),
    }


def get_vix() -> dict:
    """Pull VIX from yfinance."""
    cache_key = f"vix_{datetime.date.today()}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    try:
        import yfinance as yf
        vix = yf.Ticker("^VIX").fast_info.get("lastPrice") or 20.0
        result = {
            "vix":       round(float(vix), 2),
            "premium":   vix > 15,
            "elevated":  vix > 20,
            "extreme":   vix > 30,
            "tradeable": 12 <= vix <= 35,
        }
        _cache_set(cache_key, result)
        return result
    except Exception:
        # HIGH-012: do not return fake safe data on error
        return {"vix": None, "premium": False, "elevated": False, "extreme": False, "tradeable": False}
