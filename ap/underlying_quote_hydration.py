# ap/underlying_quote_hydration.py — P0: hydrate missing underlying at validation
# =============================================================================
# WHY THIS MODULE EXISTS
# ──────────────────────
# Forensic evidence (2026-06-29 → 2026-07-03, live Supabase):
#   • 301 signals rejected `metadata_invalid:zero_underlying` in 5 days.
#   • 0 of 301 raw payloads carried ANY underlying-price key
#     (underlying_price / underlying / underlying_at_signal / last_price).
#   • Producers: daily Strat scanner (245) + failed_dir_intraday_{1,2,3}tf (56).
#
# PR #250 (normalizer) and PR #257 (key-shadowing) both assume the value exists
# SOMEWHERE in the payload under a non-canonical key. These payloads have no
# value under any key. The scanners never send it. The guard is therefore
# correctly failing closed on genuinely missing data — but the data is cheap
# to obtain at validation time: one equity quote from the market-data broker.
#
# WHAT THIS MODULE DOES
# ─────────────────────
# Provides a process-wide, pluggable "quote resolver" that the execution core
# registers at construction time with its market-data broker. When the entry
# metadata guard fails a signal with ZERO_UNDERLYING, it calls
# try_hydrate_underlying() ONCE. If a strictly-positive price is obtained, the
# canonical fields are injected into the signal (with a provenance marker) and
# validation is re-run. If not, the existing fail-closed behavior is preserved
# byte-for-byte.
#
# DESIGN INVARIANTS
# ─────────────────
# 1. FAIL-CLOSED PRESERVED. No resolver registered → no behavior change.
#    Resolver returns None/0/negative/garbage or raises → no behavior change.
#    A signal is NEVER passed on hydration alone — it must re-pass the full
#    validate_entry_metadata() with the injected value.
# 2. NEVER OVERWRITE. If the signal already carries a positive underlying
#    under any canonical key, hydration is a no-op (the guard would not have
#    failed ZERO_UNDERLYING in that case anyway, but we re-check defensively).
# 3. PROVENANCE. Every hydrated signal is stamped:
#        underlying_hydrated_at_validation: true
#        underlying_hydration_source: "<resolver_name>"
#        underlying_hydration_ts: <iso8601>
#    so dashboards, the trade dossier, and post-trade forensics can distinguish
#    scanner-provided prices from validation-time hydration.
# 4. PAPER/LIVE AGNOSTIC. The resolver is whatever the core registers —
#    data_broker when present (paper truth path, PR #248/#227), else broker.
# 5. ONE ATTEMPT PER EVALUATE. No retry loop here; the breach-retry machinery
#    (#252/#254) owns retries. A transient quote failure surfaces as the same
#    ZERO_UNDERLYING rejection it would have been before this PR.
# 6. BOUNDED LATENCY. The resolver call is wrapped; any exception is swallowed
#    and logged at WARNING. This module adds at most one quote call to the
#    validation path and only on the failure branch.
# 7. NO IMPORT CYCLES. This module imports nothing from ap_* modules. The core
#    registers a closure; the guard imports only this module.
# =============================================================================

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

log = logging.getLogger("ap.underlying_quote_hydration")

# resolver signature: (symbol: str) -> float | None  (positive price or None)
_QuoteResolver = Callable[[str], Optional[float]]

_lock = threading.Lock()
_resolver: Optional[_QuoteResolver] = None
_resolver_name: str = ""

HYDRATION_MARKER = "underlying_hydrated_at_validation"
HYDRATION_SOURCE = "underlying_hydration_source"
HYDRATION_TS = "underlying_hydration_ts"

# Canonical keys we inject. Kept in sync with entry_metadata_guard._positive()
# accepted keys and ap_signals column `underlying_at_signal`.
_INJECT_KEYS = ("underlying_at_signal", "underlying_price")


def register_quote_resolver(fn: _QuoteResolver, *, name: str = "") -> None:
    """Register the process-wide underlying quote resolver.

    Called once by APExecutionCore at construction with a closure over its
    market-data broker. Later registrations replace earlier ones (multi-client
    cores share one process; the quote source is identical per deployment).
    """
    global _resolver, _resolver_name
    with _lock:
        _resolver = fn
        _resolver_name = name or getattr(fn, "__name__", "resolver")
    log.info("underlying_quote_hydration: resolver registered (%s)", _resolver_name)


def clear_quote_resolver() -> None:
    """Test hook / shutdown hook. Restores no-resolver (fail-closed) state."""
    global _resolver, _resolver_name
    with _lock:
        _resolver = None
        _resolver_name = ""


def resolver_registered() -> bool:
    return _resolver is not None


def _positive_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def _signal_has_positive_underlying(signal: Any) -> bool:
    """Defensive invariant-2 check across the same keys the guard accepts."""
    keys = (
        "underlying_entry", "underlying_at_signal", "underlying_price",
        "current_underlying_price", "current_underlying",
        "signal_underlying_price", "price_at_signal",
    )
    if isinstance(signal, dict):
        for k in keys:
            if _positive_float(signal.get(k)) is not None:
                return True
        trig = signal.get("trigger")
        if isinstance(trig, dict) and _positive_float(trig.get("current_price")) is not None:
            return True
        return False
    for k in keys:
        if _positive_float(getattr(signal, k, None)) is not None:
            return True
    return False


def try_hydrate_underlying(signal: Any, *, ticker: str) -> Optional[float]:
    """Attempt ONE quote fetch and inject the price into the signal.

    Returns the injected positive price on success, None on any failure.
    Never raises. Never overwrites an existing positive underlying.
    """
    if not ticker or not str(ticker).strip():
        return None
    if _signal_has_positive_underlying(signal):
        # Guard failed for a different structural reason; do not mask it.
        return None

    with _lock:
        fn = _resolver
        src = _resolver_name
    if fn is None:
        return None

    try:
        raw = fn(str(ticker).strip().upper())
    except Exception as exc:  # invariant 6: swallow, log, fail closed
        log.warning(
            "underlying_quote_hydration: resolver %s raised for %s: %s",
            src, ticker, exc,
        )
        return None

    price = _positive_float(raw)
    if price is None:
        log.warning(
            "underlying_quote_hydration: resolver %s returned non-positive for %s: %r",
            src, ticker, raw,
        )
        return None

    marker = {
        HYDRATION_MARKER: True,
        HYDRATION_SOURCE: src,
        HYDRATION_TS: datetime.now(timezone.utc).isoformat(),
    }
    if isinstance(signal, dict):
        for k in _INJECT_KEYS:
            signal[k] = price
        signal.update(marker)
        # Mirror into nested containers the guard also reads, matching the
        # write pattern of _mark_daily_underlying_data_pending().
        for container_key in ("metadata", "payload", "signal_payload"):
            container = signal.get(container_key)
            if isinstance(container, dict):
                for k in _INJECT_KEYS:
                    container[k] = price
                container.update(marker)
    else:
        for k in _INJECT_KEYS:
            try:
                setattr(signal, k, price)
            except Exception:
                pass
        for k, v in marker.items():
            try:
                setattr(signal, k, v)
            except Exception:
                pass

    log.info(
        "underlying_quote_hydration: hydrated %s underlying=%.4f source=%s",
        ticker, price, src,
    )
    return price


def build_broker_resolver(quote_broker: Any, *, name: str = "") -> _QuoteResolver:
    """Build a resolver closure over a broker exposing get_quote(symbol)->dict.

    Price preference: last, then bid/ask midpoint, then close. Zero and
    negative values are treated as absent (holiday/closed-market quotes on
    Tradier return 0 fields — those must NOT hydrate; see 2026-07-03 holiday
    signal noise).
    """
    broker_name = name or type(quote_broker).__name__

    def _resolve(symbol: str) -> Optional[float]:
        q = quote_broker.get_quote(symbol)
        if not isinstance(q, dict) or not q:
            return None
        last = _positive_float(q.get("last"))
        if last is not None:
            return last
        bid = _positive_float(q.get("bid"))
        ask = _positive_float(q.get("ask"))
        if bid is not None and ask is not None and ask >= bid:
            return (bid + ask) / 2.0
        return _positive_float(q.get("close"))

    _resolve.__name__ = f"broker_quote_resolver[{broker_name}]"
    return _resolve
