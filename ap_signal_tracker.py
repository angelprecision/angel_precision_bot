from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import yfinance as yf

from ap_signal_store import APSignalStore

log = logging.getLogger("ap.signal_tracker")
ET = ZoneInfo("America/New_York")

POLL_INTERVAL_SEC  = 60
TRACKING_WINDOW    = timedelta(minutes=90)
EOD_HOUR, EOD_MIN  = 15, 30
LOOKBACK_HOURS     = 6


class _State:
    __slots__ = (
        "signal_id", "ticker", "side", "created_at",
        "open_px", "peak_px", "trough_px", "last_px",
        "min_to_peak", "min_to_trough", "finalized",
    )

    def __init__(self, signal_id, ticker, side, created_at, open_px):
        self.signal_id     = signal_id
        self.ticker        = ticker
        self.side          = side.upper()
        self.created_at    = created_at
        self.open_px       = open_px
        self.peak_px       = open_px
        self.trough_px     = open_px
        self.last_px       = open_px
        self.min_to_peak   = 0
        self.min_to_trough = 0
        self.finalized     = False

    def update(self, price: float):
        self.last_px = price
        elapsed = int((datetime.now(timezone.utc) - self.created_at).total_seconds() / 60)
        if price > self.peak_px:
            self.peak_px     = price
            self.min_to_peak = elapsed
        if price < self.trough_px:
            self.trough_px     = price
            self.min_to_trough = elapsed

    @property
    def expires_at(self):
        return self.created_at + TRACKING_WINDOW

    def is_expired(self, now: datetime) -> bool:
        if now >= self.expires_at:
            return True
        et = now.astimezone(ET)
        cutoff = et.replace(hour=EOD_HOUR, minute=EOD_MIN, second=0, microsecond=0)
        return et >= cutoff

    def to_outcome(self, now: datetime) -> dict:
        open_px = self.open_px or 1e-9
        if self.side == "CALL":
            favorable = (self.peak_px   - open_px) / open_px * 100
            adverse   = (open_px - self.trough_px) / open_px * 100
        else:
            favorable = (open_px - self.trough_px) / open_px * 100
            adverse   = (self.peak_px   - open_px) / open_px * 100

        favorable = max(favorable, 0.0)
        adverse   = max(adverse,   0.0)

        if favorable >= 5:
            label = "runner"
        elif favorable >= 1.5:
            label = "scalp"
        elif adverse >= 1.5 and favorable < 1:
            label = "failed"
        else:
            label = "noise"

        return {
            "tracking_started_at":  self.created_at.isoformat(),
            "tracking_ended_at":    now.isoformat(),
            "underlying_open":      round(self.open_px,   4),
            "underlying_peak":      round(self.peak_px,   4),
            "underlying_trough":    round(self.trough_px, 4),
            "underlying_close":     round(self.last_px,   4),
            "max_favorable_pct":    round(favorable, 3),
            "max_adverse_pct":      round(adverse,   3),
            "minutes_to_peak":      self.min_to_peak,
            "minutes_to_trough":    self.min_to_trough,
            "hit_1pct":             favorable >= 1.0,
            "hit_2pct":             favorable >= 2.0,
            "hit_3pct":             favorable >= 3.0,
            "hit_5pct":             favorable >= 5.0,
            "failed_immediately":   adverse >= 1.0 and favorable < 0.5 and self.min_to_trough <= 15,
            "outcome_label":        label,
        }


class APSignalTracker:
    def __init__(self, supabase_client=None, store: Optional[APSignalStore] = None):
        self.sb    = supabase_client
        self.store = store or APSignalStore(supabase_client)
        self._states: dict[str, _State] = {}
        self._lock    = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="ap-signal-tracker"
        )
        self._thread.start()
        log.info("APSignalTracker started")

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self._tick()
            except Exception as exc:
                log.warning("Signal tracker tick failed: %s", exc)
            time.sleep(POLL_INTERVAL_SEC)

    def _tick(self):
        now = datetime.now(timezone.utc)
        self._load_new_signals(now)

        with self._lock:
            active = {sid: s for sid, s in self._states.items() if not s.finalized}

        if not active:
            return

        ticker_map: dict[str, list[_State]] = {}
        for state in active.values():
            ticker_map.setdefault(state.ticker, []).append(state)

        prices = self._batch_prices(set(ticker_map))

        for ticker, states in ticker_map.items():
            price = prices.get(ticker)
            if price is None:
                continue
            for state in states:
                # Set open_px on first price observation if not set at signal time
                if state.open_px == 0:
                    state.open_px  = price
                    state.peak_px  = price
                    state.trough_px = price
                state.update(price)

        to_finalize: list[_State] = []
        with self._lock:
            for sid, state in list(self._states.items()):
                if not state.finalized and state.is_expired(now):
                    state.finalized = True
                    to_finalize.append(state)
                    del self._states[sid]

        for state in to_finalize:
            outcome = state.to_outcome(now)
            self.store.upsert_underlying_outcome(state.signal_id, outcome)
            log.info(
                "[%s] Finalized | favorable=%.2f%% adverse=%.2f%% label=%s",
                state.ticker,
                outcome["max_favorable_pct"],
                outcome["max_adverse_pct"],
                outcome["outcome_label"],
            )

    def _load_new_signals(self, now: datetime):
        if self.sb is None:
            return
        horizon = (now - timedelta(hours=LOOKBACK_HOURS)).isoformat()
        try:
            q = (
                self.sb.table("ap_signals")
                .select("signal_id,ticker,side,created_at,underlying_at_signal")
                .gte("created_at", horizon)
                .not_.is_("ticker", "null")
                .execute()
            )
            rows = q.data or []
        except Exception as e:
            log.warning("Failed to fetch signals: %s", e)
            return

        try:
            done_q = (
                self.sb.table("ap_signal_underlying_outcomes")
                .select("signal_id")
                .not_.is_("tracking_ended_at", "null")
                .execute()
            )
            done_ids = {r["signal_id"] for r in (done_q.data or [])}
        except Exception:
            done_ids = set()

        with self._lock:
            existing_ids = set(self._states)

        for row in rows:
            sid = row.get("signal_id")
            if not sid or sid in existing_ids or sid in done_ids:
                continue
            ticker = row.get("ticker")
            if not ticker:
                continue
            created_at = _parse_ts(row.get("created_at"))
            if created_at is None:
                continue
            open_px = float(row.get("underlying_at_signal") or 0)
            with self._lock:
                self._states[sid] = _State(
                    signal_id=sid, ticker=ticker,
                    side=(row.get("side") or "CALL"),
                    created_at=created_at, open_px=open_px,
                )

    @staticmethod
    def _batch_prices(tickers: set[str]) -> dict[str, float]:
        if not tickers:
            return {}
        prices: dict[str, float] = {}
        ticker_list = list(tickers)
        try:
            df = yf.download(
                tickers=ticker_list, period="1d", interval="1m",
                group_by="ticker", progress=False, auto_adjust=True, threads=True,
            )
            if df.empty:
                raise ValueError("empty")
            for t in ticker_list:
                try:
                    col_df = df if len(ticker_list) == 1 else (df[t] if t in df.columns.get_level_values(0) else None)
                    if col_df is None or col_df.empty:
                        continue
                    close = col_df["Close"].dropna()
                    if not close.empty:
                        prices[t] = round(float(close.iloc[-1]), 4)
                except Exception:
                    pass
            return prices
        except Exception:
            pass
        for ticker in ticker_list:
            try:
                hist = yf.Ticker(ticker).history(period="1d", interval="1m")
                if not hist.empty:
                    prices[ticker] = round(float(hist["Close"].dropna().iloc[-1]), 4)
            except Exception:
                pass
        return prices


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None
