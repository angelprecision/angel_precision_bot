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
import math
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

# ── JSON boundary serialization ──────────────────────────────────────────────
# NaN values from yfinance (missing PE, earnings, etc.) must survive the cache
# cycle as float('nan') so scoring agents can apply their own isnan() guards.
# Replacing NaN→None changes scoring semantics: agents that do
# `if math.isnan(val): skip` would instead receive None, treating missing
# data as a concrete value (often 0 or negative), collapsing scores.
#
# Fix: encode NaN/Inf as sentinel dicts at write time; decode back at read time.
# Timestamps are encoded as ISO strings (safe; agents don't use them as floats).
# No other data transformation happens — scoring values are never modified here.

_NAN_SENTINEL     = {"__nan__": True}
_INF_SENTINEL     = {"__inf__": True}
_NEG_INF_SENTINEL = {"__neginf__": True}


def _nan_preprocess(data):
    """Pre-process before json.dump: NaN/Inf→sentinel dicts.
    json.dumps' default= callback is never invoked for float — it serializes
    them natively as 'NaN' (invalid JSON). Pre-processing converts them to
    dicts first so json.dump produces valid, round-trippable JSON.
    Does NOT modify in-memory data; only the value passed to json.dump changes.
    """
    import math
    if isinstance(data, float):
        if math.isnan(data):  return {"__nan__": True}
        if math.isinf(data):  return {"__inf__": True} if data > 0 else {"__neginf__": True}
        return data
    if isinstance(data, dict):
        return {k: _nan_preprocess(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_nan_preprocess(v) for v in data]
    return data


def _nan_safe_encoder(obj):
    """JSON default= handler: Timestamp→ISO, unknown types→str.
    Called only for types json.dumps cannot handle natively (not floats).
    NaN floats are handled by _nan_preprocess before this runs.
    """
    import datetime
    try:
        import pandas as _pd
        if isinstance(obj, _pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, _pd.Series):
            return obj.tolist()
    except ImportError:
        pass
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    return str(obj)


def _nan_safe_decode(data):
    """Decode NaN sentinels back to float('nan')/float('inf') after json.load."""
    if isinstance(data, dict):
        if len(data) == 1:
            if data == _NAN_SENTINEL:     return float("nan")
            if data == _INF_SENTINEL:     return float("inf")
            if data == _NEG_INF_SENTINEL: return float("-inf")
        return {k: _nan_safe_decode(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_nan_safe_decode(v) for v in data]
    return data


def _cache_get(key: str):
    p = _cache_path(key)
    if p.exists():
        age = time.time() - p.stat().st_mtime
        if age < 3600:  # 1 hour TTL
            try:
                with open(p) as f:
                    raw = json.load(f)
                # Restore NaN sentinels → float('nan') so scoring agents see NaN
                return _nan_safe_decode(raw)
            except (json.JSONDecodeError, ValueError):
                # Corrupt cache — delete and let caller fetch fresh
                try: p.unlink()
                except Exception: pass
    return None


def _cache_set(key: str, data):
    p = _cache_path(key)
    with open(p, "w") as f:
        # Pre-process converts NaN→sentinel dicts (json.dumps never calls default= for floats).
        # Then _nan_safe_encoder handles Timestamps and any other non-standard types.
        # In-memory data is not modified — only the value passed to json.dump changes.
        json.dump(_nan_preprocess(data), f, default=_nan_safe_encoder)

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

        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True,
                         progress=False, group_by="ticker")
        if df.empty:
            return pd.DataFrame()
        # yfinance MultiIndex flatten: with group_by="ticker" it returns tuples
        # like ("NVDA", "Close") — ticker FIRST, field SECOND. Previous code took
        # c[0] which gave the ticker name for every column and collapsed OHLCV
        # into one duplicate column. Pick whichever tuple element matches a known
        # OHLCV field name so we're robust to both layouts.
        _OHLCV = {"open", "high", "low", "close", "adj close", "volume"}
        def _pick(c):
            if isinstance(c, tuple):
                for part in c:
                    if str(part).lower() in _OHLCV:
                        return str(part).lower()
                # Fallback: last element (yfinance group_by="ticker" layout)
                return str(c[-1]).lower()
            return str(c).lower()
        df.columns = [_pick(c) for c in df.columns]
        # Remove duplicate columns (MultiIndex flatten can create them)
        df = df.loc[:, ~df.columns.duplicated()]
        df.index.name = "Date"
        _cache_set(cache_key, df.reset_index().assign(**{"Date": lambda x: x["Date"].astype(str)}).to_dict(orient="records"))
        return df
    except Exception as e:
        log.warning("get_prices_yfinance(%s) failed: %s", ticker, e)  # MED-009
        return pd.DataFrame()


# Module-level price cache — survives across calls within same process
_last_known_prices: dict = {}  # ticker -> DataFrame (last valid result)


def get_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Main entry: Tradier first, yfinance fallback, last-known cache last resort."""
    df = get_prices_tradier(ticker, start_date, end_date)
    if df.empty:
        df = get_prices_yfinance(ticker, start_date, end_date)

    if not df.empty:
        # Normalize column names to lowercase so downstream code always finds 'close'
        df.columns = [str(c).lower() for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()]
        # Guarantee a 'close' column exists — yfinance with auto_adjust=True can emit
        # 'adj close' only; Tradier edge cases can drop fields too. Alias the closest
        # available price column so downstream df['close'] never KeyErrors.
        if "close" not in df.columns:
            for _alias in ("adj close", "adjclose", "adj_close", "last", "price"):
                if _alias in df.columns:
                    df["close"] = df[_alias]
                    log.warning("[%s] get_prices: aliased '%s' -> 'close'", ticker, _alias)
                    break
            else:
                # No usable close column — treat as no data rather than return a broken frame
                log.warning("[%s] get_prices: no close-like column in %s — discarding",
                            ticker, list(df.columns))
                df = pd.DataFrame()
        if not df.empty:
            # Cache the valid result for this ticker
            _last_known_prices[ticker] = df
            return df

    # Both sources failed — use last known price if available
    cached = _last_known_prices.get(ticker)
    if cached is not None and not cached.empty:
        log.warning("[%s] PRICE DATA FAILED — using last known cache (%d rows)", ticker, len(cached))
        return cached

    # No data at all — return stub so intel doesn't crash, just scores low
    log.warning("[%s] PRICE DATA FAILED — returning stub, intel will use scanner score only", ticker)
    return pd.DataFrame()


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
    if df.empty or len(df) < lookback or "close" not in df.columns:
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
    # A cached VIX value is only eligible for the hard market-safety gate when
    # it carries an observation timestamp.  Older cache records without that
    # attestation are treated as unavailable rather than as a safe default.
    if isinstance(cached, dict) and cached.get("observed_at"):
        return cached

    try:
        import yfinance as yf
        raw_vix = yf.Ticker("^VIX").fast_info.get("lastPrice")
        vix = float(raw_vix)
        if not math.isfinite(vix) or vix <= 0:
            raise ValueError("VIX lastPrice missing or non-finite")
        observed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        result = {
            "vix":       round(float(vix), 2),
            "premium":   vix > 15,
            "elevated":  vix > 20,
            "extreme":   vix > 30,
            "tradeable": 12 <= vix <= 35,
            "source":     "yfinance:^VIX.fast_info.lastPrice",
            "observed_at": observed_at,
            "classification": "PRODUCTION_EXACT",
        }
        _cache_set(cache_key, result)
        return result
    except Exception:
        # HIGH-012: do not return fake safe data on error.
        return {
            "vix": None,
            "premium": False,
            "elevated": False,
            "extreme": False,
            "tradeable": False,
            "source": "yfinance:^VIX.fast_info.lastPrice",
            "observed_at": None,
            "classification": "UNAVAILABLE",
        }
