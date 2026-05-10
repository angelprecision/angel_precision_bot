"""
ap_quote_authority.py
=====================
Enforces that ONLY ap_position_quote_monitor writes quote fields.

The Exit Engine is a PURE CONSUMER — it reads snapshots, never writes.
Any other caller attempting to write is logged as an error and rejected.

This solves the stale-quote-overwrite race condition where the exit engine's
independent fetch loop was overwriting QPM's fresher data with stale data.

Usage in QPM (the authorized writer):
    from ap_quote_authority import QUOTES
    QUOTES.write(
        writer_id="ap_position_quote_monitor",
        symbol=contract_symbol,
        underlying=ticker,
        bid=bid, ask=ask, last=last,
        underlying_price=underlying_px,
    )

Usage in Exit Engine (consumer):
    from ap_quote_authority import QUOTES
    snap = QUOTES.get_fresh(position.option_symbol, max_age_s=12)
    if not snap:
        log.warning("[EXIT] No fresh quote for %s — holding", position.option_symbol)
        continue
    current_price = snap.mid
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

log = logging.getLogger("ap.quotes")

# Only this module ID may write quote data.
AUTHORIZED_WRITER = "ap_position_quote_monitor"

# Default max age for get_fresh() if caller does not specify.
DEFAULT_STALE_THRESHOLD_S = 10.0


@dataclass(frozen=True)
class QuoteSnapshot:
    """Immutable point-in-time quote. Written only by QPM."""
    symbol:            str
    underlying:        str
    bid:               float
    ask:               float
    mid:               float
    last:              float
    underlying_price:  float
    timestamp_epoch:   float
    source:            str

    def age_s(self) -> float:
        return time.time() - self.timestamp_epoch if self.timestamp_epoch else 9e9

    def is_stale(self, threshold_s: float = DEFAULT_STALE_THRESHOLD_S) -> bool:
        return self.age_s() > threshold_s


class QuoteAuthority:
    """
    Singleton quote store.
    Single-writer enforcement: only AUTHORIZED_WRITER may call write().
    All other components call get_fresh() or get() — read-only.
    """

    _instance: Optional["QuoteAuthority"] = None
    _class_lock = threading.Lock()

    def __new__(cls) -> "QuoteAuthority":
        with cls._class_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._quotes: Dict[str, QuoteSnapshot] = {}
                inst._quote_lock = threading.RLock()
                inst._reject_count = 0
                cls._instance = inst
            return cls._instance

    # ------------------------------------------------------------------
    # Write (QPM only)
    # ------------------------------------------------------------------

    def write(
        self,
        writer_id: str,
        symbol: str,
        underlying: str,
        bid: float,
        ask: float,
        last: float,
        underlying_price: float,
        source: str = "tradier",
    ) -> bool:
        """
        Write a quote snapshot. Only AUTHORIZED_WRITER may call this.
        Returns True on success, False if caller is not authorized.
        """
        if writer_id != AUTHORIZED_WRITER:
            with self._quote_lock:
                self._reject_count += 1
            log.error(
                "[QUOTE_AUTHORITY] REJECTED write from '%s' for %s. "
                "Only '%s' may write quote data.",
                writer_id, symbol, AUTHORIZED_WRITER,
            )
            return False

        bid_f  = float(bid  or 0)
        ask_f  = float(ask  or 0)
        last_f = float(last or 0)
        mid    = round((bid_f + ask_f) / 2.0, 4) if bid_f > 0 and ask_f > 0 else last_f

        with self._quote_lock:
            self._quotes[str(symbol).upper()] = QuoteSnapshot(
                symbol           = str(symbol).upper(),
                underlying       = str(underlying).upper(),
                bid              = bid_f,
                ask              = ask_f,
                mid              = mid,
                last             = last_f,
                underlying_price = float(underlying_price or 0),
                timestamp_epoch  = time.time(),
                source           = source,
            )
        return True

    # ------------------------------------------------------------------
    # Read (everyone)
    # ------------------------------------------------------------------

    def get_fresh(
        self,
        symbol: str,
        max_age_s: Optional[float] = None,
    ) -> Optional[QuoteSnapshot]:
        """
        Return snapshot only if it is fresher than max_age_s.
        Returns None if missing or stale — caller must hold, not guess.
        """
        threshold = max_age_s if max_age_s is not None else DEFAULT_STALE_THRESHOLD_S
        with self._quote_lock:
            snap = self._quotes.get(str(symbol).upper())
        if snap is None:
            return None
        return snap if not snap.is_stale(threshold) else None

    def get(self, symbol: str) -> Optional[QuoteSnapshot]:
        """Return the latest snapshot regardless of age. None if never written."""
        with self._quote_lock:
            return self._quotes.get(str(symbol).upper())

    def snapshot_all(self) -> Dict[str, dict]:
        """Summary of all tracked quotes for dashboard / health endpoints."""
        now = time.time()
        with self._quote_lock:
            return {
                sym: {
                    "bid":        s.bid,
                    "ask":        s.ask,
                    "mid":        s.mid,
                    "underlying": s.underlying_price,
                    "age_s":      round(now - s.timestamp_epoch, 1),
                    "stale":      s.is_stale(),
                    "source":     s.source,
                }
                for sym, s in self._quotes.items()
            }

    @property
    def reject_count(self) -> int:
        with self._quote_lock:
            return self._reject_count


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
QUOTES = QuoteAuthority()
