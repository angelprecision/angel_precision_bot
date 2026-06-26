"""
ap_entry_confirmation.py
========================
P0 Follow-up: Pre-submit entry confirmation preflight.

Called from APExecutionCore._on_entry_trigger() AFTER:
  1. _breach_risk_check() passes
  2. contract selected (deferred resolved)
  3. live ask refreshed via _refresh_ask_at_submit()

And BEFORE submit_existing_entry() — the broker call.

DESIGN PRINCIPLE
----------------
This is NOT a sleep/delay loop. The watcher already confirmed the
price held for MOMENTUM_POLLS_REQUIRED ticks. This preflight is a
single synchronous check: are conditions STILL clean right now?

For daily / 1d signals this also requires fresh intraday continuation
context before submit. Missing continuation context fails closed so a
one-touch opening move cannot drift into broker submit as a clean entry.

WHEN confirmation_required IS NOT SET
-------------------------------------
The legacy option quote confirmation remains a no-op for non-client plans,
but the daily continuation guard still runs for daily / 1d entries. This
keeps the new gate entry-only and active before broker submit without
changing broker, queue, handoff, order submit/cancel, or position logic.

ENV FLAGS (hot-read per call)
-----------------------------
ENTRY_CONFIRM_SECONDS            = 45   (max quote age treated as fresh)
MAX_PRE_ENTRY_OPTION_FADE_PCT    = 8    (block if option faded > 8%)
MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT = 0.25  (block if price moved > 0.25% wrong way)
CLIENT_PROOF_MAX_SPREAD_PCT      = 0.10 (block if spread > 10%)
CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS = 10 (block if quote older than 10s)
ENABLE_DAILY_CONTINUATION_VALIDATION = true (daily continuation guard)
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Sequence

log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

def _ef(key: str, default: float) -> float:
    try: return float(os.getenv(key, str(default)))
    except: return default


def _env_enabled(key: str, default: bool = True) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _tier_confirm_seconds(score: Optional[float], tier: Optional[str],
                          timeframe: Optional[str]) -> float:
    """
    Tier-based confirmation window (used as max quote age).
    Faster for high-conviction setups — we want those trades to go through.
    """
    base = _ef("ENTRY_CONFIRM_SECONDS", 45.0)
    tf   = (timeframe or "1d").lower()
    s    = float(score or 0)
    is_intraday = any(tf.startswith(x) for x in ("15", "30", "60"))

    if is_intraday:
        # Intraday: stricter — but don't use a fixed long wait
        return max(base, _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0))

    if s >= 78:           # A+ daily: quote must be fresh (10-15s)
        return min(base, 15.0)
    elif s >= 70:         # B-tier daily: 30-45s
        return min(base, 45.0)
    return base           # fallback


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class ConfirmationResult:
    passed: bool
    fail_reason: Optional[str]
    metadata: dict = field(default_factory=dict)

    def to_meta(self, started_at: str, completed_at: str) -> dict:
        return {
            "confirmation_required":    True,
            "confirmation_seconds":     self.metadata.get("confirmation_seconds"),
            "confirmation_started_at":  started_at,
            "confirmation_completed_at":completed_at,
            "confirmation_passed":      self.passed,
            "confirmation_fail_reason": self.fail_reason,
            **{k: v for k, v in self.metadata.items()
               if k not in ("confirmation_seconds",)},
        }


def _fail(reason: str, meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] BLOCKED reason=%s", reason)
    return ConfirmationResult(passed=False, fail_reason=reason, metadata=meta)


def _pass(meta: dict) -> ConfirmationResult:
    log.info("[ENTRY_CONFIRM] PASSED")
    return ConfirmationResult(passed=True, fail_reason=None, metadata=meta)


# ── Daily continuation helpers ────────────────────────────────────────────────

_DAILY_TIMEFRAMES = {"1d", "d", "day", "daily", "overnight"}


def _is_daily_timeframe(timeframe: Optional[str]) -> bool:
    return str(timeframe or "").strip().lower() in _DAILY_TIMEFRAMES


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _plan_get(plan: Any, *names: str, default: Any = None) -> Any:
    if plan is None:
        return default
    if isinstance(plan, Mapping):
        for name in names:
            if name in plan:
                return plan.get(name)
        return default
    for name in names:
        if hasattr(plan, name):
            return getattr(plan, name)
    return default


def _extract_plan_metadata(plan: Any) -> dict:
    if hasattr(plan, "metadata"):
        raw = getattr(plan, "metadata") or {}
    elif isinstance(plan, Mapping):
        raw = plan.get("metadata") or {}
    else:
        raw = {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _first_sequence(*values: Any) -> Sequence[Any] | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (str, bytes, Mapping)):
            continue
        if isinstance(value, Sequence):
            return value
    return None


def _extract_intraday_candles(meta_src: Mapping[str, Any]) -> Sequence[Any] | None:
    intraday_context = _as_mapping(
        meta_src.get("intraday_context")
        or meta_src.get("daily_continuation_context")
        or meta_src.get("continuation_context")
    )
    return _first_sequence(
        meta_src.get("intraday_candles"),
        meta_src.get("continuation_candles"),
        meta_src.get("daily_continuation_candles"),
        intraday_context.get("candles"),
        intraday_context.get("intraday_candles"),
        intraday_context.get("bars"),
    )


def _run_daily_continuation_guard(
    *,
    plan: Any,
    meta_src: Mapping[str, Any],
    direction: str,
    trigger_price: Optional[float],
    underlying_last: Optional[float],
    timeframe: Optional[str],
) -> ConfirmationResult | None:
    """Run the PR #197 validator inside the existing pre-submit seam.

    Returns None when the signal is not daily/1d or when the guard is explicitly
    disabled. Otherwise returns a pass/fail ConfirmationResult. This helper has
    no broker, order, position, queue, proof_trade, submit, or cancel side
    effects; APExecutionCore already owns terminalization for failed preflights.
    """
    if not _is_daily_timeframe(timeframe):
        return None

    if not _env_enabled("ENABLE_DAILY_CONTINUATION_VALIDATION", True):
        return ConfirmationResult(
            passed=True,
            fail_reason=None,
            metadata={
                "daily_continuation": {
                    "validator": "daily_intraday_continuation",
                    "continuation_allowed": True,
                    "reason": "daily_continuation_skipped:disabled",
                    "timeframe": timeframe,
                }
            },
        )

    ticker = (
        _plan_get(plan, "ticker", "symbol", default=None)
        or meta_src.get("ticker")
        or meta_src.get("symbol")
    )
    client_id = (
        _plan_get(plan, "client_id", default=None)
        or meta_src.get("client_id")
        or meta_src.get("client_email")
    )
    execution_mode = (
        _plan_get(plan, "execution_mode", default=None)
        or meta_src.get("execution_mode")
    )
    canonical_signal_id = (
        _plan_get(plan, "canonical_signal_id", "signal_id", default=None)
        or meta_src.get("canonical_signal_id")
        or meta_src.get("signal_id")
    )
    invalidation_price = (
        _plan_get(plan, "stop_underlying", "stop_price", default=None)
        or meta_src.get("stop_underlying")
        or meta_src.get("stop_price")
        or _as_mapping(meta_src.get("trigger")).get("stop")
    )
    candles = _extract_intraday_candles(meta_src)

    try:
        from ap.daily_continuation_validator import validate_daily_intraday_continuation
    except Exception as exc:
        meta = {
            "daily_continuation": {
                "validator": "daily_intraday_continuation",
                "continuation_allowed": False,
                "reason": "daily_continuation_failed:missing_intraday_context",
                "import_error": type(exc).__name__,
                "ticker": ticker,
                "timeframe": timeframe,
                "direction": direction,
                "client_id": client_id,
                "execution_mode": execution_mode,
                "canonical_signal_id": canonical_signal_id,
            }
        }
        return _fail("daily_continuation_failed:missing_intraday_context", meta)

    decision = validate_daily_intraday_continuation(
        ticker=str(ticker or "") or None,
        timeframe=timeframe,
        direction=direction,
        trigger_price=trigger_price,
        current_underlying_price=underlying_last,
        intraday_candles=candles,
        client_id=str(client_id or "") or None,
        execution_mode=str(execution_mode or "") or None,
        canonical_signal_id=str(canonical_signal_id or "") or None,
        option_premium_now=None,
        option_premium_prev=None,
        invalidation_price=invalidation_price,
        extra_diagnostics={
            "adapter": "entry_confirmation_preflight",
            "metadata_candles_present": bool(candles),
            "source": "plan.metadata",
        },
    )
    meta = {"daily_continuation": dict(decision.diagnostics)}
    if not decision.allowed:
        return _fail(decision.reason, meta)
    return ConfirmationResult(passed=True, fail_reason=None, metadata=meta)


# ── Main preflight ────────────────────────────────────────────────────────────

def check_entry_confirmation(
    *,
    plan,                            # APTradePlan or dict with .metadata / .get()
    direction: str,                  # "CALL" or "PUT"
    trigger_price: Optional[float],
    live_bid: Optional[float],       # option bid at submit time (from _refresh_ask_at_submit)
    live_ask: Optional[float],       # option ask at submit time
    live_quote_age_ms: Optional[float],  # ms since quote was fetched
    underlying_last: Optional[float],    # current underlying price
    decision_option_price: Optional[float],  # option price at signal decision time
    score: Optional[float] = None,
    tier: Optional[str]   = None,
    timeframe: Optional[str] = None,
    sandbox_mode: bool = False,
) -> ConfirmationResult:
    """
    Pre-submit confirmation preflight.

    Checks (in order, fail-fast):
    1. Daily / 1d intraday continuation, when applicable
    2. confirmation_required present in plan metadata — skip legacy quote checks if absent
    3. Quote freshness
    4. Spread acceptability
    5. Option fade from decision price
    6. Underlying reversal from trigger

    Returns immediately on first failure (don't waste time on subsequent checks).
    """
    # ── Resolve plan metadata ─────────────────────────────────────────────
    meta_src = _extract_plan_metadata(plan)

    gate_meta = meta_src.get('hybrid_client_quality_gate') or {}
    confirmation_required = gate_meta.get('confirmation_required', False)

    started_at   = datetime.now(timezone.utc).isoformat()
    max_fade     = _ef("MAX_PRE_ENTRY_OPTION_FADE_PCT",       8.0)
    max_reversal = _ef("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)
    max_spread   = _ef("CLIENT_PROOF_MAX_SPREAD_PCT",         0.10)
    max_quote_age_s = _ef("CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS", 10.0)
    confirm_s    = _tier_confirm_seconds(score, tier, timeframe)
    dirn         = direction.upper()

    live_mid = None
    spread_pct = None
    if live_bid is not None and live_ask is not None and live_bid > 0 and live_ask > 0:
        live_mid   = (live_bid + live_ask) / 2
        spread_pct = (live_ask - live_bid) / live_mid if live_mid > 0 else None

    quote_age_s = (live_quote_age_ms or 0) / 1000.0

    base = {
        "confirmation_required":    bool(confirmation_required),
        "confirmation_seconds":       confirm_s,
        "underlying_start":           trigger_price,
        "underlying_end":             underlying_last,
        "option_mid_start":           decision_option_price,
        "option_mid_end":             live_mid,
        "option_move_pct":            None,
        "underlying_move_pct":        None,
        "quote_age_seconds":          round(quote_age_s, 2),
        "spread_pct":                 round(spread_pct, 4) if spread_pct is not None else None,
        "live_entry_bid":             live_bid,
        "live_entry_ask":             live_ask,
        "live_entry_mid":             live_mid,
        "live_entry_ts":              started_at,
        "paper_quote_lag_warning":    False,
        "sandbox_mode":               sandbox_mode,
    }

    # ── 0. Daily continuation guard ───────────────────────────────────────
    daily_continuation_result = _run_daily_continuation_guard(
        plan=plan,
        meta_src=meta_src,
        direction=dirn,
        trigger_price=trigger_price,
        underlying_last=underlying_last,
        timeframe=timeframe,
    )
    if daily_continuation_result is not None:
        base.update(daily_continuation_result.metadata)
        if not daily_continuation_result.passed:
            return _fail(daily_continuation_result.fail_reason or "daily_continuation_failed", base)

    # ── Fast path: legacy quote confirmation not required ─────────────────
    if not confirmation_required:
        return ConfirmationResult(
            passed=True,
            fail_reason=None,
            metadata=base,
        )

    # ── 1. Quote age ──────────────────────────────────────────────────────
    # Use the stricter of confirm_s and CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS
    effective_max_age = min(confirm_s, max_quote_age_s)
    if live_bid is None and live_ask is None:
        return _fail("entry_confirm_failed_stale_quote",
                     {**base, "reason": "no live quote available at submit time"})

    if quote_age_s > effective_max_age:
        return _fail("entry_confirm_failed_stale_quote",
                     {**base,
                      "quote_age_seconds": round(quote_age_s, 2),
                      "max_age_seconds":   effective_max_age})

    # ── 2. Spread ─────────────────────────────────────────────────────────
    if spread_pct is not None and spread_pct > max_spread:
        return _fail("entry_confirm_failed_spread",
                     {**base,
                      "spread_pct":     round(spread_pct, 4),
                      "max_spread_pct": max_spread})

    # ── 3. Option fade ────────────────────────────────────────────────────
    if decision_option_price and decision_option_price > 0 and live_mid is not None:
        fade_pct = (decision_option_price - live_mid) / decision_option_price * 100
        base["option_move_pct"] = round(fade_pct, 2)
        if fade_pct > max_fade:
            return _fail("entry_confirm_failed_option_fade",
                         {**base,
                          "fade_pct":     round(fade_pct, 2),
                          "max_fade_pct": max_fade,
                          "decision_price": decision_option_price,
                          "live_mid":       live_mid})

        # Paper quote lag warning: if sandbox and live quotes differ materially
        if sandbox_mode and fade_pct > 2.0:
            base["paper_quote_lag_warning"] = True

    # ── 4. Underlying reversal ────────────────────────────────────────────
    if underlying_last is not None and trigger_price is not None and trigger_price > 0:
        move_pct = (underlying_last - trigger_price) / trigger_price * 100
        base["underlying_move_pct"] = round(move_pct, 2)

        if dirn == "CALL":
            # Block if underlying has pulled back below trigger by > threshold
            reversal = -move_pct  # positive means pulled back
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last":  underlying_last,
                              "trigger_price":    trigger_price,
                              "reversal_pct":     round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "CALL"})
        elif dirn == "PUT":
            # Block if underlying has reclaimed above trigger by > threshold
            reversal = move_pct   # positive means reclaimed above
            if reversal > max_reversal:
                return _fail("entry_confirm_failed_underlying_reversal",
                             {**base,
                              "underlying_last":  underlying_last,
                              "trigger_price":    trigger_price,
                              "reversal_pct":     round(reversal, 3),
                              "max_reversal_pct": max_reversal,
                              "direction": "PUT"})

    # ── All checks passed ─────────────────────────────────────────────────
    return _pass(base)
