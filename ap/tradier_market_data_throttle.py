"""
ap/tradier_market_data_throttle.py

Purpose
-------
Process-wide Tradier market-data call throttle. Prevents chain/quote stampede
when multiple deferred PENDING_TRIGGER rows breach close together during the
9:30–9:40 market-open warmup window, when Tradier chains are not yet fully
populated. Without throttling, concurrent selector calls to underlying quotes,
expirations, and option chains can exhaust Tradier's rate limits, returning
CHAIN_PROVIDER_EMPTY_EXPIRATIONS, CHAIN_PROVIDER_ERROR, CHAIN_ROW_ZERO_BID_ASK,
or DIRECT_QUOTE_ZERO_BID_ASK — leaving rows as DEFERRED:*/0.01.

Scope
-----
This module wraps ONLY known market-data endpoints:
  - /v1/markets/quotes         (underlying quote in _fetch_tradier_chain)
  - /v1/markets/options/expirations   (both _fetch_tradier_chain and
                                       _fetch_expirations_list)
  - /v1/markets/options/chains        (_fetch_tradier_chain)
  - direct OCC quote in               (fetch_direct_option_quote_with_meta)

It does NOT touch:
  - broker order submission (submit_existing_entry, execute, place_order)
  - fill monitor
  - exit engine
  - _refresh_ask_at_submit (sits immediately before broker POST; excluded)
  - scanner ingestion
  - dashboards or scoring

Provider failures are NEVER hidden. If Tradier returns 429, 401, 403, 500,
empty expirations, empty options, or zero bid/ask, the caller still sees the
original error response and the existing reason taxonomy fires unchanged
(CHAIN_PROVIDER_ERROR, CHAIN_PROVIDER_EMPTY_EXPIRATIONS, CHAIN_ROW_ZERO_BID_ASK,
DIRECT_QUOTE_RATE_LIMITED, etc.). This module delays calls; it never converts
provider failure into success or generic UNKNOWN.

Env controls
------------
TRADIER_MD_THROTTLE_ENABLED       0 | 1          (default: 0 — disabled, no-op)
TRADIER_MD_MIN_INTERVAL_MS        integer         (default: 250 ms)
TRADIER_MD_OPEN_MIN_INTERVAL_MS   integer         (default: 500 ms)
TRADIER_MD_OPEN_WINDOW_START_ET   HH:MM           (default: 09:30)
TRADIER_MD_OPEN_WINDOW_END_ET     HH:MM           (default: 09:40)
TRADIER_MD_JITTER_MS              integer         (default: 75 ms)
TRADIER_MD_MAX_CONCURRENT         reserved for future use — currently ignored

When TRADIER_MD_THROTTLE_ENABLED is 0 (default), before_market_data_call()
is a complete no-op: no sleep, no lock, no logging. Every other code path
is byte-for-byte identical to pre-PR behavior.
"""

from __future__ import annotations

import os
import random
import threading
import time
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.tradier_md_throttle")

_ET = ZoneInfo("America/New_York")

# ── Process-wide shared state (module-level singletons) ───────────────────────
# All state is protected by _LOCK. The guarantee this module provides is
# call-START spacing: no two market-data GETs begin closer together than the
# configured minimum interval. It does NOT limit how many in-flight requests
# may be running concurrently (that would require a separate semaphore, reserved
# for a future PR once spacing alone is proven in production).
_LOCK = threading.Lock()
_LAST_CALL_TS: float = 0.0


def _cfg() -> dict:
    """Read throttle config from env — hot-read every call so Render env
    changes take effect without a restart (useful for tuning under load)."""
    def _ms(key: str, default: int) -> int:
        try:
            return max(0, int(os.getenv(key, str(default)).strip()))
        except (TypeError, ValueError):
            return default

    def _hhmm(key: str, default: str) -> str:
        val = os.getenv(key, default).strip()
        # Validate HH:MM format; fall back to default on garbage value.
        try:
            h, m = val.split(":")
            if 0 <= int(h) <= 23 and 0 <= int(m) <= 59:
                return val
        except Exception:
            pass
        return default

    def _int(key: str, default: int) -> int:
        try:
            return max(1, int(os.getenv(key, str(default)).strip()))
        except (TypeError, ValueError):
            return default

    return {
        "enabled":          os.getenv("TRADIER_MD_THROTTLE_ENABLED", "0").strip() in ("1", "true", "yes"),
        "min_ms":           _ms("TRADIER_MD_MIN_INTERVAL_MS", 250),
        "open_ms":          _ms("TRADIER_MD_OPEN_MIN_INTERVAL_MS", 500),
        "open_start":       _hhmm("TRADIER_MD_OPEN_WINDOW_START_ET", "09:30"),
        "open_end":         _hhmm("TRADIER_MD_OPEN_WINDOW_END_ET", "09:40"),
        "jitter_ms":        _ms("TRADIER_MD_JITTER_MS", 75),
        # TRADIER_MD_MAX_CONCURRENT is reserved for a future concurrent-limiting
        # semaphore. In this PR it is parsed but NOT used — present only so
        # operators who set it don't silently get wrong behavior from a future
        # release.
        "max_concurrent_reserved": _int("TRADIER_MD_MAX_CONCURRENT", 1),
    }


def _in_open_window(cfg: dict) -> bool:
    """Return True if current ET time is inside the open-window override band."""
    try:
        now_et = datetime.now(_ET)
        now_t = now_et.hour * 60 + now_et.minute
        sh, sm = cfg["open_start"].split(":")
        eh, em = cfg["open_end"].split(":")
        start_t = int(sh) * 60 + int(sm)
        end_t   = int(eh) * 60 + int(em)
        return start_t <= now_t < end_t
    except Exception:
        return False


def before_market_data_call(
    endpoint: str,
    symbol: str,
    context: str = "",
) -> None:
    """
    Call this IMMEDIATELY before any Tradier market-data GET request.

    When TRADIER_MD_THROTTLE_ENABLED=0 (default) this is a complete no-op.

    When enabled:
      1. Computes the interval since the last market-data call start.
      2. If elapsed < required_interval + random jitter, sleeps the deficit.
      3. Optimistically reserves the next slot by advancing _LAST_CALL_TS,
         so concurrent callers queue rather than pile up.
      4. Emits one structured log line with wait_ms.

    After the HTTP request completes (or fails), callers MUST call
    after_market_data_call() in a finally block to update _LAST_CALL_TS
    with max(reserved, actual_now), preserving any future reservation made
    by a concurrently-waiting thread.

    Note: this provides call-START spacing, not in-flight concurrency limiting.
    A future PR may add a semaphore for the latter once spacing alone is
    validated in production.

    Provider errors are NEVER suppressed. If Tradier returns 429 or empty
    data, the caller still receives the original response and the existing
    reason taxonomy fires (CHAIN_PROVIDER_ERROR, DIRECT_QUOTE_RATE_LIMITED,
    etc.). This function only delays the request; it never intercepts results.
    """
    global _LAST_CALL_TS

    cfg = _cfg()
    if not cfg["enabled"]:
        return  # ← complete no-op; behavior unchanged for disabled state

    required_ms   = cfg["open_ms"] if _in_open_window(cfg) else cfg["min_ms"]
    jitter_ms     = random.randint(0, max(0, cfg["jitter_ms"]))
    total_min_ms  = required_ms + jitter_ms

    with _LOCK:
        now_ts       = time.monotonic()
        elapsed_ms   = int((now_ts - _LAST_CALL_TS) * 1000)
        wait_ms      = max(0, total_min_ms - elapsed_ms)
        _LAST_CALL_TS = now_ts + (wait_ms / 1000.0)  # reserve the slot optimistically

    if wait_ms > 0:
        log.info(
            "TRADIER_MD_THROTTLE_WAIT endpoint=%s symbol=%s wait_ms=%d "
            "required_ms=%d jitter_ms=%d context=%s",
            endpoint, symbol, wait_ms, required_ms, jitter_ms, context,
        )
        time.sleep(wait_ms / 1000.0)


def after_market_data_call() -> None:
    """Update last-call timestamp after the HTTP request completes.

    Uses max() rather than unconditional assignment so a concurrent thread's
    already-reserved future slot is never overwritten backward. Example:
      Thread A starts at t=0.00, reserves slot t=0.00
      Thread B starts at t=0.05, reserves future slot t=0.50
      Thread A finishes at t=0.10 → after_market_data_call writes max(0.50, 0.10) = 0.50
      Thread B's reservation is preserved; Thread C must wait until t≥0.50

    Call in a finally block after every before_market_data_call().
    """
    global _LAST_CALL_TS
    cfg = _cfg()
    if not cfg["enabled"]:
        return
    with _LOCK:
        _LAST_CALL_TS = max(_LAST_CALL_TS, time.monotonic())
