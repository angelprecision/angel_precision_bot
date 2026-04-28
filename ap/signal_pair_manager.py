# ap/signal_pair_manager.py -- Angel Precision | 1-1 Signal Pair Manager
# =============================================================================
# Handles the 1-1 (inside bar) problem:
#   Scanner sends BOTH a CALL and PUT for the same ticker/date
#   Bot must only enter the ONE that breaches first
#   The other side must be auto-canceled the moment one fills
#
# How it works:
#   1. When a 1-1 CALL and 1-1 PUT arrive for the same ticker,
#      register them as a pair
#   2. Both go into the entry watcher normally
#   3. When one BREACHES and fills, immediately cancel the other
#   4. If neither breaches before EOD, both expire naturally
#
# This prevents: buying both a CALL and PUT on CVX simultaneously
# =============================================================================

from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timezone
from typing import Optional

log = logging.getLogger("ap.signal_pair_manager")


class SignalPairManager:
    """
    Tracks 1-1 signal pairs (inside bar setups that fire both CALL and PUT).
    When one side fills, the other is immediately invalidated.

    Thread-safe — used by entry_watcher and fill_monitor concurrently.
    """

    def __init__(self):
        self._lock  = threading.Lock()
        # pairs: {pair_key: {"CALL": local_order_id, "PUT": local_order_id, "filled": None}}
        self._pairs: dict[str, dict] = {}

    def _make_pair_key(self, ticker: str, trade_date: str) -> str:
        return f"{ticker}:{trade_date}"

    def register(
        self,
        ticker: str,
        side: str,
        local_order_id: str,
        pattern_id: str = "",
        trade_date: str = "",
    ) -> Optional[str]:
        """
        Register a signal as part of a 1-1 pair.
        Returns pair_key if this is a 1-1 pattern, None otherwise.

        Call this from queue._dispatch() when a signal has pattern_id
        containing '1-1' or '1_1'.
        """
        is_pair_pattern = any(p in (pattern_id or "") for p in ("1-1", "1_1", "inside"))
        if not is_pair_pattern:
            return None

        if not trade_date:
            trade_date = date.today().isoformat()

        pair_key = self._make_pair_key(ticker, trade_date)

        with self._lock:
            if pair_key not in self._pairs:
                self._pairs[pair_key] = {
                    "CALL":   None,
                    "PUT":    None,
                    "filled": None,
                    "ticker": ticker,
                    "date":   trade_date,
                }
            self._pairs[pair_key][side.upper()] = local_order_id
            log.info(
                "[PAIR] Registered %s %s | order=%s | pair=%s",
                ticker, side, local_order_id, pair_key,
            )

        return pair_key

    def on_fill(
        self,
        ticker: str,
        side: str,
        local_order_id: str,
        trade_date: str = "",
    ) -> Optional[str]:
        """
        Called when one side of a pair fills.
        Returns the local_order_id of the OTHER side to cancel,
        or None if no pair registered.

        Call this from fill_monitor or OSM on FILLED transition.
        """
        if not trade_date:
            trade_date = date.today().isoformat()

        pair_key = self._make_pair_key(ticker, trade_date)

        with self._lock:
            pair = self._pairs.get(pair_key)
            if not pair:
                return None

            if pair["filled"]:
                log.debug("[PAIR] %s already filled by %s — ignoring", pair_key, pair["filled"])
                return None

            pair["filled"] = side.upper()
            other_side     = "PUT" if side.upper() == "CALL" else "CALL"
            other_order_id = pair.get(other_side)

            if other_order_id:
                log.warning(
                    "[PAIR] %s %s FILLED — canceling opposite %s order=%s",
                    ticker, side, other_side, other_order_id,
                )
                return other_order_id

        return None

    def is_canceled(self, ticker: str, side: str, trade_date: str = "") -> bool:
        """
        Returns True if the other side of this pair already filled,
        meaning this side should be canceled.
        """
        if not trade_date:
            trade_date = date.today().isoformat()

        pair_key = self._make_pair_key(ticker, trade_date)

        with self._lock:
            pair = self._pairs.get(pair_key)
            if not pair:
                return False
            filled = pair.get("filled")
            if not filled:
                return False
            return filled != side.upper()

    def cleanup_old_pairs(self, days_old: int = 2) -> int:
        """Remove pairs older than N days. Call daily."""
        cutoff = date.today().isoformat()
        removed = 0
        with self._lock:
            to_remove = [
                k for k, v in self._pairs.items()
                if v.get("date", "9999") < cutoff
            ]
            for k in to_remove:
                del self._pairs[k]
                removed += 1
        if removed:
            log.info("[PAIR] Cleaned up %d old pairs", removed)
        return removed

    def get_status(self) -> dict:
        """Return current pair state for debugging."""
        with self._lock:
            return {
                k: {
                    "CALL":   v.get("CALL"),
                    "PUT":    v.get("PUT"),
                    "filled": v.get("filled"),
                }
                for k, v in self._pairs.items()
            }


# Singleton instance — import and use this everywhere
_pair_manager: Optional[SignalPairManager] = None
_pm_lock = threading.Lock()

def get_pair_manager() -> SignalPairManager:
    global _pair_manager
    if _pair_manager is None:
        with _pm_lock:
            if _pair_manager is None:
                _pair_manager = SignalPairManager()
    return _pair_manager
