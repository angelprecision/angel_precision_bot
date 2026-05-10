# ap/market_intelligence.py
# =============================================================================
# Earnings Blackout Gate + IV Rank Filter
#
# Classes:
#   APEarningsGuard   — blocks trades within N days of earnings events
#   APIVRankFilter    — blocks trades when implied volatility rank is too high
#
# Both classes:
#   - Read credentials from broker.cfg.token / broker.cfg.base_url
#     with fallback to broker.token / broker.base_url
#   - Never crash on API failure (fail open)
#   - Log via logging.getLogger("ap.market_intelligence")
#   - No hardcoded credentials
#   - SQL would use %s placeholders (Postgres standard)
# =============================================================================

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("ap.market_intelligence")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_broker_creds(broker) -> Tuple[str, str]:
    """
    Extract API token and base_url from broker object.
    Prefers broker.cfg.token / broker.cfg.base_url; falls back to
    broker.token / broker.base_url.
    Returns (token, base_url).
    """
    cfg = getattr(broker, "cfg", None)
    token = (
        getattr(cfg, "token", None)
        or getattr(broker, "token", "")
    )
    base_url = (
        getattr(cfg, "base_url", None)
        or getattr(broker, "base_url", "https://sandbox.tradier.com")
    )
    return str(token or ""), str(base_url or "https://sandbox.tradier.com")


def _tradier_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


# =============================================================================
# 1. APEarningsGuard
# =============================================================================

class APEarningsGuard:
    """
    Checks Tradier's market calendar / fundamentals API for upcoming earnings
    events and blocks option trades within a configurable blackout window.

    Fail-open contract: any API error returns blocked=False (with a warning)
    so that infrastructure faults never silently prevent trading.

    Cache: earnings dates are cached per ticker for 4 hours (in-memory).

    Usage
    -----
    guard = APEarningsGuard(broker, blackout_days=3)
    result = guard.check("AAPL")
    # {
    #   "blocked": True,
    #   "reason": "earnings in 2 days (2026-04-14)",
    #   "earnings_date": "2026-04-14",
    #   "days_until": 2,
    # }
    """

    _CACHE_TTL_SECONDS: int = 4 * 3600  # 4 hours

    def __init__(self, broker, blackout_days: int = 3) -> None:
        self.broker = broker
        self.blackout_days = blackout_days
        # Cache: {ticker: (fetched_at_epoch, earnings_date_str_or_None)}
        self._cache: Dict[str, Tuple[float, Optional[str]]] = {}
        log.info(
            "APEarningsGuard | blackout_days=%d cache_ttl=%dh",
            blackout_days,
            self._CACHE_TTL_SECONDS // 3600,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check(self, ticker: str) -> Dict[str, Any]:
        """
        Check whether ticker is within the earnings blackout window.

        Returns a dict:
          blocked       bool
          reason        str
          earnings_date str | None   ("YYYY-MM-DD")
          days_until    int | None
        """
        ticker = ticker.upper().strip()

        # Serve from cache if still fresh
        cached = self._cache.get(ticker)
        if cached is not None:
            fetched_at, earnings_date_str = cached
            if time.time() - fetched_at < self._CACHE_TTL_SECONDS:
                log.debug("[%s] EarningsGuard: cache hit", ticker)
                return self._evaluate(ticker, earnings_date_str)

        # Fetch fresh data
        earnings_date_str = self._fetch_earnings_date(ticker)

        # Store in cache regardless of result (including None)
        self._cache[ticker] = (time.time(), earnings_date_str)

        return self._evaluate(ticker, earnings_date_str)

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def _evaluate(
        self, ticker: str, earnings_date_str: Optional[str]
    ) -> Dict[str, Any]:
        """Decide block/pass given an earnings date string (or None)."""
        if earnings_date_str is None:
            return {
                "blocked": False,
                "reason": "no upcoming earnings found",
                "earnings_date": None,
                "days_until": None,
            }

        try:
            earnings_date = date.fromisoformat(earnings_date_str)
        except ValueError:
            log.warning(
                "[%s] EarningsGuard: could not parse earnings date %r",
                ticker,
                earnings_date_str,
            )
            return {
                "blocked": False,
                "reason": f"could not parse earnings date {earnings_date_str!r}",
                "earnings_date": earnings_date_str,
                "days_until": None,
            }

        today = date.today()
        days_until = (earnings_date - today).days

        if 0 <= days_until <= self.blackout_days:
            reason = (
                f"earnings in {days_until} day(s) ({earnings_date_str}) "
                f"— within {self.blackout_days}-day blackout"
            )
            log.warning("[%s] EarningsGuard: BLOCKED — %s", ticker, reason)
            return {
                "blocked": True,
                "reason": reason,
                "earnings_date": earnings_date_str,
                "days_until": days_until,
            }

        return {
            "blocked": False,
            "reason": (
                f"earnings on {earnings_date_str} ({days_until} days away) "
                f"— outside {self.blackout_days}-day blackout"
            ),
            "earnings_date": earnings_date_str,
            "days_until": days_until,
        }

    # ------------------------------------------------------------------
    # Data fetch — Tradier then yfinance fallback
    # ------------------------------------------------------------------

    def _fetch_earnings_date(self, ticker: str) -> Optional[str]:
        """
        Try Tradier fundamentals API first; fall back to yfinance.
        Any exception → return None (fail open).
        """
        result = self._fetch_from_tradier(ticker)
        if result is not None:
            return result

        result = self._fetch_from_yfinance(ticker)
        return result

    def _fetch_from_tradier(self, ticker: str) -> Optional[str]:
        """
        Query Tradier GET /v1/markets/fundamentals/calendars for upcoming
        earnings. Returns the nearest future earnings date string or None.
        """
        try:
            import requests

            token, base_url = _get_broker_creds(self.broker)
            headers = _tradier_headers(token)

            resp = requests.get(
                f"{base_url}/v1/markets/fundamentals/calendars",
                params={"symbols": ticker},
                headers=headers,
                timeout=8,
            )

            if resp.status_code != 200:
                log.debug(
                    "[%s] EarningsGuard: Tradier fundamentals/calendars returned %d",
                    ticker,
                    resp.status_code,
                )
                return None

            data = resp.json()
            # Tradier wraps results in a list; each item may contain calendar data
            if isinstance(data, list):
                for item in data:
                    earnings_str = self._extract_tradier_earnings(ticker, item)
                    if earnings_str:
                        return earnings_str
            elif isinstance(data, dict):
                return self._extract_tradier_earnings(ticker, data)

        except Exception as exc:
            log.warning(
                "[%s] EarningsGuard: Tradier API error — %s (fail open)",
                ticker,
                exc,
            )
        return None

    def _extract_tradier_earnings(
        self, ticker: str, data: dict
    ) -> Optional[str]:
        """
        Parse Tradier fundamentals/calendars response for the nearest
        upcoming earnings date. Returns ISO date string or None.
        """
        try:
            today = date.today()
            best: Optional[date] = None

            # Navigate nested structure: data -> request -> results -> tables -> ...
            # The actual structure varies; try multiple paths.
            tables = (
                data.get("tables")
                or data.get("results", {}).get("tables")
                or {}
            )

            # earnings_events table
            earning_events = tables.get("earning_events") or []
            if isinstance(earning_events, dict):
                earning_events = [earning_events]

            for event in earning_events:
                event_date_str = (
                    event.get("report_date")
                    or event.get("date")
                    or event.get("event_date")
                )
                if not event_date_str:
                    continue
                try:
                    event_date = date.fromisoformat(str(event_date_str)[:10])
                    if event_date >= today:
                        if best is None or event_date < best:
                            best = event_date
                except (ValueError, TypeError):
                    continue

            if best:
                return best.isoformat()

        except Exception as exc:
            log.debug(
                "[%s] EarningsGuard: error parsing Tradier response — %s",
                ticker,
                exc,
            )
        return None

    def _fetch_from_yfinance(self, ticker: str) -> Optional[str]:
        """
        Fallback: use yfinance earnings_dates property to find the next
        upcoming earnings date. Returns ISO date string or None.
        """
        try:
            import yfinance as yf  # type: ignore

            yt = yf.Ticker(ticker)
            earnings_dates = getattr(yt, "earnings_dates", None)
            if earnings_dates is None or earnings_dates.empty:
                log.debug(
                    "[%s] EarningsGuard: yfinance returned no earnings_dates",
                    ticker,
                )
                return None

            today = date.today()
            best: Optional[date] = None

            for idx in earnings_dates.index:
                try:
                    # Index is a DatetimeTZDtype; convert to plain date
                    if hasattr(idx, "date"):
                        ed = idx.date()
                    else:
                        ed = date.fromisoformat(str(idx)[:10])
                    if ed >= today:
                        if best is None or ed < best:
                            best = ed
                except Exception:
                    continue

            if best:
                log.debug(
                    "[%s] EarningsGuard: yfinance found earnings %s",
                    ticker,
                    best.isoformat(),
                )
                return best.isoformat()

        except ImportError:
            log.debug(
                "[%s] EarningsGuard: yfinance not installed, skipping fallback",
                ticker,
            )
        except Exception as exc:
            log.warning(
                "[%s] EarningsGuard: yfinance error — %s (fail open)",
                ticker,
                exc,
            )
        return None


# =============================================================================
# 2. APIVRankFilter
# =============================================================================

class APIVRankFilter:
    """
    Computes IV rank (current IV vs 52-week range) from Tradier option chain
    greeks and blocks trades when IV is historically expensive.

    IV rank = (current_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100

    Blocks if iv_rank > max_iv_rank (default 70).

    Fallback when historical range is unavailable:
      Block if mid_iv > 0.80 (absolute 80% IV threshold).

    Cache: IV rank per ticker cached for 30 minutes (in-memory dict).

    Usage
    -----
    iv_filter = APIVRankFilter(broker, max_iv_rank=70)
    result = iv_filter.check("AAPL", option_chain=chain_data, underlying_price=180.0)
    # {
    #   "blocked": True,
    #   "reason": "IV rank 84 > max 70 (current_iv=0.72 range=[0.22, 0.89])",
    #   "iv_rank": 84,
    #   "current_iv": 0.72,
    #   "iv_52w_low": 0.22,
    #   "iv_52w_high": 0.89,
    # }
    """

    _CACHE_TTL_SECONDS: int = 30 * 60  # 30 minutes
    _ABSOLUTE_IV_BLOCK_THRESHOLD: float = 0.80  # fallback when no history

    def __init__(self, broker, max_iv_rank: float = 70.0, hard_cap: float = None, mode: str = "RESEARCH") -> None:
        self.broker      = broker
        self.mode        = (mode or "RESEARCH").upper()
        # Zone thresholds by mode:
        #
        # PAPER/RESEARCH:
        #   Zone 1 (normal):     IV rank ≤ max_iv_rank (105 from env) → free pass
        #   Zone 2 (exceeding):  max_iv_rank < IV rank ≤ hard_cap (150) → requires momentum score≥65
        #   Zone 3 (extreme):    IV rank > 150 → hard block
        #
        # LIVE/PROD (tighter):
        #   Zone 1 (normal):     IV rank ≤ min(max_iv_rank, 100) → free pass
        #   Zone 2 (soft):       100 < IV rank ≤ 120 → requires momentum score≥65
        #   Zone 3 (hard):       120 < IV rank ≤ 140 → requires strong momentum score≥72
        #   Zone 4 (extreme):    IV rank > 140 → hard block
        if self.mode in ("LIVE", "PROD"):
            self.max_iv_rank = min(float(max_iv_rank), 100.0)
            self.soft_cap    = self.max_iv_rank   # env var respected in live too
            self.hard_cap    = hard_cap if hard_cap is not None else 120.0
            self.extreme_cap = 140.0
        else:
            self.max_iv_rank = float(max_iv_rank)
            # Wire env var to soft_cap so MAX_IV_RANK=105 actually gates zone 1
            self.soft_cap    = self.max_iv_rank
            self.hard_cap    = hard_cap if hard_cap is not None else 150.0
            # Ceiling: anything above hard_cap (150) is blocked immediately
            # extreme_cap = hard_cap + 1 collapses zones 3+4 → one hard block above ceiling
            self.extreme_cap = self.hard_cap + 1.0  # e.g. 151 when hard_cap=150
        # Cache: {ticker: (fetched_at_epoch, result_dict)}
        self._cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        log.info(
            "APIVRankFilter | mode=%s soft=%.0f hard=%.0f extreme=%.0f cache_ttl=%dm",
            self.mode, self.soft_cap, self.hard_cap, self.extreme_cap,
            self._CACHE_TTL_SECONDS // 60,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def check(
        self,
        ticker: str,
        option_chain: list,
        underlying_price: float,
    ) -> Dict[str, Any]:
        """
        Evaluate IV rank for a ticker.

        Parameters
        ----------
        ticker           : str   — e.g. "AAPL"
        option_chain     : list  — list of option dicts from Tradier chain API
                                   (must include greeks.mid_iv per option)
        underlying_price : float — current underlying price for ATM detection

        Returns
        -------
        dict with keys: blocked, reason, iv_rank, current_iv,
                        iv_52w_low, iv_52w_high
        """
        ticker = ticker.upper().strip()

        # Serve from cache if still fresh
        cached = self._cache.get(ticker)
        if cached is not None:
            fetched_at, cached_result = cached
            if time.time() - fetched_at < self._CACHE_TTL_SECONDS:
                log.debug("[%s] IVRankFilter: cache hit iv_rank=%s", ticker, cached_result.get("iv_rank"))
                return cached_result

        result = self._compute(ticker, option_chain, underlying_price)
        self._cache[ticker] = (time.time(), result)
        return result

    # ------------------------------------------------------------------
    # Core computation
    # ------------------------------------------------------------------

    def _compute(
        self,
        ticker: str,
        option_chain: list,
        underlying_price: float,
    ) -> Dict[str, Any]:
        """Compute IV rank and decide block/pass."""
        # Step 1: extract current IV from ATM options
        current_iv = self._get_atm_iv(option_chain, underlying_price)
        if current_iv is None:
            log.warning(
                "[%s] IVRankFilter: could not extract ATM IV from chain (fail open)",
                ticker,
            )
            return self._open_result("could not extract ATM IV from chain")

        # Step 2: get 52-week IV range
        iv_52w_low, iv_52w_high = self._get_iv_range(ticker)

        # Step 3: compute rank or fall back to absolute threshold
        if iv_52w_low is None or iv_52w_high is None or iv_52w_high <= iv_52w_low:
            # Fallback: absolute IV threshold
            log.debug(
                "[%s] IVRankFilter: no historical IV range — using absolute threshold %.0f%%",
                ticker,
                self._ABSOLUTE_IV_BLOCK_THRESHOLD * 100,
            )
            if current_iv > self._ABSOLUTE_IV_BLOCK_THRESHOLD:
                reason = (
                    f"IV {current_iv:.2f} ({current_iv*100:.0f}%) exceeds absolute threshold "
                    f"{self._ABSOLUTE_IV_BLOCK_THRESHOLD*100:.0f}% (no 52w range available)"
                )
                log.warning("[%s] IVRankFilter: BLOCKED — %s", ticker, reason)
                result = {
                    "blocked": True,
                    "reason": reason,
                    "iv_rank": None,
                    "current_iv": round(current_iv, 4),
                    "iv_52w_low": None,
                    "iv_52w_high": None,
                }
            else:
                result = {
                    "blocked": False,
                    "reason": (
                        f"IV {current_iv:.2f} ({current_iv*100:.0f}%) within absolute "
                        f"threshold {self._ABSOLUTE_IV_BLOCK_THRESHOLD*100:.0f}%"
                    ),
                    "iv_rank": None,
                    "current_iv": round(current_iv, 4),
                    "iv_52w_low": None,
                    "iv_52w_high": None,
                }
            return result

        # Standard IV rank calculation
        iv_rank = (current_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100
        iv_rank = round(iv_rank, 1)

        result: Dict[str, Any] = {
            "blocked": False,
            "reason": "",
            "iv_rank": iv_rank,
            "current_iv": round(current_iv, 4),
            "iv_52w_low": round(iv_52w_low, 4),
            "iv_52w_high": round(iv_52w_high, 4),
        }

        ctx = f"(current_iv={current_iv:.2f} range=[{iv_52w_low:.2f}, {iv_52w_high:.2f}])"

        if iv_rank > self.extreme_cap:
            # Zone 4: extreme IV — always reject
            reason = f"IV rank {iv_rank:.0f} > extreme_cap {self.extreme_cap:.0f} {ctx}"
            log.warning("[%s] IVGate reject | iv=%.1f reason=iv_extreme", ticker, iv_rank)
            result["blocked"]           = True
            result["reason"]            = reason
            result["iv_zone"]           = "extreme"
            result["requires_momentum"] = False

        elif iv_rank > self.hard_cap:
            # Zone 3: hard cap zone — allow only with STRONG momentum (score>=72, momentum>=0.60)
            reason = f"IV rank {iv_rank:.0f} in hard zone ({self.hard_cap:.0f}–{self.extreme_cap:.0f}) {ctx}"
            log.info("[%s] IVGate hard_zone | iv=%.1f — requires strong momentum (score>=72, mom>=0.60)", ticker, iv_rank)
            result["blocked"]           = False
            result["tier_cap"]          = "B"
            result["reason"]            = reason
            result["iv_zone"]           = "hard"
            result["requires_momentum"] = True
            result["momentum_min_score"]= 72.0
            result["momentum_min_pct"]  = 0.60

        elif iv_rank > self.soft_cap:
            # Zone 2: soft zone — allow with moderate momentum (score>=65, momentum>=0.40)
            reason = f"IV rank {iv_rank:.0f} in soft zone ({self.soft_cap:.0f}–{self.hard_cap:.0f}) {ctx}"
            log.info("[%s] IVGate soft_zone | iv=%.1f — requires momentum (score>=65, mom>=0.40)", ticker, iv_rank)
            result["blocked"]           = False
            result["tier_cap"]          = "B"
            result["reason"]            = reason
            result["iv_zone"]           = "soft"
            result["requires_momentum"] = True
            result["momentum_min_score"]= 65.0
            result["momentum_min_pct"]  = 0.40

        else:
            # Zone 1: normal IV — allow freely
            result["reason"]            = f"IV rank {iv_rank:.0f} <= soft_cap {self.soft_cap:.0f} {ctx}"
            result["iv_zone"]           = "normal"
            result["requires_momentum"] = False
            log.debug("[%s] IVGate allow | iv=%.1f zone=normal", ticker, iv_rank)

        return result

    # ------------------------------------------------------------------
    # ATM IV extraction from chain
    # ------------------------------------------------------------------

    def _get_atm_iv(
        self, option_chain: list, underlying_price: float
    ) -> Optional[float]:
        """
        Find ATM options (call + put closest to underlying_price),
        average their mid_iv from greeks.
        Returns float or None.
        """
        if not option_chain or not underlying_price or underlying_price <= 0:
            return None

        try:
            # Separate calls and puts
            calls = [o for o in option_chain if o.get("option_type", "").lower() == "call"]
            puts  = [o for o in option_chain if o.get("option_type", "").lower() == "put"]

            ivs: list[float] = []

            for side in (calls, puts):
                if not side:
                    continue
                # Pick the strike closest to underlying_price
                atm = min(
                    side,
                    key=lambda o: abs(float(o.get("strike") or 0) - underlying_price),
                )
                greeks = atm.get("greeks") or {}
                mid_iv = greeks.get("mid_iv")
                if mid_iv is not None:
                    try:
                        v = float(mid_iv)
                        if v > 0:
                            ivs.append(v)
                    except (ValueError, TypeError):
                        pass

            if not ivs:
                return None
            return sum(ivs) / len(ivs)

        except Exception as exc:
            log.debug("_get_atm_iv error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Historical IV range — Tradier then yfinance
    # ------------------------------------------------------------------

    def _get_iv_range(
        self, ticker: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Attempt to get 52-week high/low IV.
        1. Tradier historical volatility endpoint
        2. yfinance rolling realized volatility as proxy
        Returns (iv_low, iv_high) or (None, None).
        """
        result = self._tradier_iv_range(ticker)
        if result != (None, None):
            return result
        return self._yfinance_iv_range(ticker)

    def _tradier_iv_range(
        self, ticker: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Try Tradier GET /v1/markets/history?symbol=X&interval=daily&start=...
        to compute rolling IV range from historical data.
        """
        try:
            import requests

            token, base_url = _get_broker_creds(self.broker)
            headers = _tradier_headers(token)

            end_date = date.today()
            start_date = end_date - timedelta(days=365)

            resp = requests.get(
                f"{base_url}/v1/markets/history",
                params={
                    "symbol": ticker,
                    "interval": "daily",
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                },
                headers=headers,
                timeout=10,
            )

            if resp.status_code != 200:
                log.debug(
                    "[%s] IVRankFilter: Tradier history returned %d",
                    ticker,
                    resp.status_code,
                )
                return (None, None)

            data = resp.json()
            history = data.get("history", {})
            if not history:
                return (None, None)

            day_list = history.get("day") or []
            if isinstance(day_list, dict):
                day_list = [day_list]

            # Extract closing prices and compute 30-day rolling realized vol
            closes = []
            for day in day_list:
                try:
                    closes.append(float(day["close"]))
                except (KeyError, TypeError, ValueError):
                    continue

            if len(closes) < 21:
                return (None, None)

            import math as _math

            def _rolling_vol(prices: list, window: int = 21) -> list:
                """Annualised close-to-close vol for each window."""
                vols = []
                for i in range(window, len(prices)):
                    slice_ = prices[i - window: i]
                    log_rets = [
                        _math.log(slice_[j] / slice_[j - 1])
                        for j in range(1, len(slice_))
                    ]
                    mean = sum(log_rets) / len(log_rets)
                    variance = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
                    vols.append(_math.sqrt(variance * 252))
                return vols

            vols = _rolling_vol(closes)
            if not vols:
                return (None, None)

            return (min(vols), max(vols))

        except Exception as exc:
            log.warning(
                "[%s] IVRankFilter: Tradier history error — %s",
                ticker,
                exc,
            )
        return (None, None)

    def _yfinance_iv_range(
        self, ticker: str
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        Fallback: use yfinance to compute 52-week realized volatility range
        as a proxy for IV range.
        """
        try:
            import yfinance as yf  # type: ignore

            yt = yf.Ticker(ticker)
            hist = yt.history(period="1y", interval="1d")

            if hist is None or hist.empty or len(hist) < 22:
                return (None, None)

            import math as _math

            closes = list(hist["Close"])

            def _rolling_vol(prices: list, window: int = 21) -> list:
                vols = []
                for i in range(window, len(prices)):
                    slice_ = prices[i - window: i]
                    log_rets = [
                        _math.log(slice_[j] / slice_[j - 1])
                        for j in range(1, len(slice_))
                    ]
                    mean = sum(log_rets) / len(log_rets)
                    variance = sum((r - mean) ** 2 for r in log_rets) / (len(log_rets) - 1)
                    vols.append(_math.sqrt(variance * 252))
                return vols

            vols = _rolling_vol(closes)
            if not vols:
                return (None, None)

            log.debug(
                "[%s] IVRankFilter: yfinance vol range [%.2f, %.2f]",
                ticker,
                min(vols),
                max(vols),
            )
            return (min(vols), max(vols))

        except ImportError:
            log.debug(
                "[%s] IVRankFilter: yfinance not installed, no IV range",
                ticker,
            )
        except Exception as exc:
            log.warning(
                "[%s] IVRankFilter: yfinance IV range error — %s",
                ticker,
                exc,
            )
        return (None, None)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _open_result(reason: str) -> Dict[str, Any]:
        """Return a fail-open (not blocked) result."""
        return {
            "blocked": False,
            "reason": reason,
            "iv_rank": None,
            "current_iv": None,
            "iv_52w_low": None,
            "iv_52w_high": None,
        }
