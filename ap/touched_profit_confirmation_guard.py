"""Require durable executable-bid confirmation before a LIVE touched-profit exit.

The native exit engine intentionally evaluates hard stops, confirmed underlying
stops, EOD exits, and runner trails before or independently from the low-profit
``TOUCHED_PROFIT_STOP`` branch.  Those authorities remain unchanged.

This guard owns one narrow production seam: a LIVE or mode-unproven position
that merely armed ``touched_profit`` must not be fully liquidated from one
transient executable BID observation below its profit floor.  Canonical PAPER
positions remain unchanged.  The candidate exit must be reproduced on a
configurable number of *distinct* BID observations.  A recovered quote resets
the confirmation state.  Once the BID breach is confirmed, a still-positive
pre-runner winner also receives the exit engine's bounded pullback-recovery
window; the timer starts at the first real floor breach, not at entry.

The July 27 NVDA incident is the motivating production shape:

* peak executable BID P&L: about +6%;
* one later BID observation implied about -15%;
* the broker filled the exit near +5%, proving the low BID was not durable;
* the option subsequently traded near +47% over entry.

The QQQ winner from the same session is deliberately outside this guard.  It
used ``RUNNER_TRAIL`` after peaking about +53% and exited about +41%; runner
trails pass through byte-for-byte.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.touched_profit_confirmation_guard")

_PATCHED_ATTR = "_AP_TOUCHED_PROFIT_CONFIRMATION_PATCHED"
_ORIGINAL_ATTR = "_AP_TOUCHED_PROFIT_CONFIRMATION_ORIGINAL"

_STATE_COUNT = "_touched_profit_floor_breach_count"
_STATE_KEY = "_touched_profit_floor_breach_observation_key"
_STATE_STARTED = "_touched_profit_floor_breach_started_at"


def _enabled() -> bool:
    """Default ON because this is a quote-flicker money-safety correction."""
    return str(
        os.getenv("TOUCHED_PROFIT_BREACH_CONFIRMATION_ENABLED", "1")
    ).strip().lower() in {"1", "true", "yes", "on"}


def _required_confirmations() -> int:
    """Return a bounded confirmation count; two distinct bids is the default."""
    raw = os.getenv("TOUCHED_PROFIT_BREACH_CONFIRMATIONS", "2")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = 2
    return max(2, min(4, value))


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    if result != result or result in (float("inf"), float("-inf")):
        return default
    return result


def _normalize_ts(value: Any) -> str:
    """Produce a stable identity for the dedicated BID observation timestamp."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=timezone.utc)
        else:
            normalized = normalized.astimezone(timezone.utc)
        return normalized.isoformat(timespec="microseconds")
    return str(value).strip()


def _observation_key(pos: Any) -> str:
    """Key a candidate breach to the dedicated BID observation timestamp.

    A changed numeric BID with the same timestamp is still one observation, not
    independent confirmation.  Missing timestamps intentionally collapse to the
    same key; the guard then keeps holding rather than certifying a durable soft
    exit without quote provenance.
    """
    bid_ts = getattr(
        pos,
        "last_option_bid_update_ts",
        getattr(pos, "lastoptionbidupdatets", None),
    )
    return _normalize_ts(bid_ts) or "missing_bid_timestamp"


def _reset_state(pos: Any) -> None:
    setattr(pos, _STATE_COUNT, 0)
    setattr(pos, _STATE_KEY, "")
    setattr(pos, _STATE_STARTED, None)


def _prepare_pullback_recovery(pos: Any, decision: Any, now_et=None):
    """Start/continue the shared bounded recovery timer for a live candidate."""
    try:
        current_pnl = _float(getattr(decision, "pnl_pct", 0.0))
        if current_pnl <= 0.0:
            return None

        import ap_exit_engine as engine_module

        peak_pnl = max(
            _float(getattr(pos, "peak_pnl_pct", 0.0)),
            _float(getattr(pos, "max_profit_seen", 0.0)),
        )
        try:
            session_date = now_et.astimezone(engine_module.ET).date() if now_et else None
        except Exception:
            session_date = None
        _, runner_arm, _ = engine_module._effective_thresholds(
            pos, session_date=session_date,
        )
        if not (0.0 < peak_pnl < float(runner_arm)):
            engine_module._reset_profit_pullback_state(pos)
            return None

        floor = engine_module._profit_floor_for_peak(peak_pnl)
        if current_pnl > floor:
            engine_module._reset_profit_pullback_state(pos)
            return None

        confirming, confirming_reason = engine_module._underlying_still_confirming(pos)
        if not confirming:
            engine_module._reset_profit_pullback_state(pos)
            return None

        now_utc = engine_module._evaluation_now_utc(now_et, pos=pos)
        # Establish the timer on the first fresh candidate.  Calling the
        # helper here also validates persisted state without producing an
        # extra decision before BID-confirmation is complete.
        return engine_module._hold_for_profit_pullback_recovery(
            pos,
            now_utc=now_utc,
            current_pnl=current_pnl,
            floor=floor,
            peak_pnl=peak_pnl,
            confirming_reason=confirming_reason,
        )
    except Exception as exc:
        # A recovery-policy failure must not rewrite the existing
        # broker-truth/confirmation decision.
        log.debug("profit pullback recovery helper unavailable: %s", exc)
        return None


def _decision_code(decision: Any, classify_decision: Callable[[Any], str]) -> str:
    try:
        code = classify_decision(decision)
    except Exception:
        code = getattr(decision, "reason_code", "")
    return str(code or "").strip().upper()


def _is_protected_touched_profit_stop(pos: Any, decision: Any, code: str) -> bool:
    mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
    protected_mode = mode != "paper"
    return bool(
        protected_mode
        and code == "TOUCHED_PROFIT_STOP"
        and str(getattr(decision, "action", "") or "").strip().upper() == "CLOSE_ALL"
    )


def wrap_evaluate_exit(
    original: Callable[..., Any],
    *,
    exit_decision_cls: type,
    classify_decision: Callable[[Any], str],
) -> Callable[..., Any]:
    """Wrap ``evaluate_exit`` without changing any non-touched-profit decision."""

    def guarded(pos, now_et=None):
        decision = original(pos, now_et=now_et)

        if not _enabled():
            _reset_state(pos)
            return decision

        code = _decision_code(decision, classify_decision)
        if not _is_protected_touched_profit_stop(pos, decision, code):
            # A recovered quote returns HOLD, while genuine hard/runner/EOD exits
            # return their own code. Either way, an interrupted sequence is not
            # durable and must restart from observation one.
            _reset_state(pos)
            return decision

        observation_key = _observation_key(pos)
        if observation_key == "missing_bid_timestamp":
            _reset_state(pos)
            return exit_decision_cls(
                action="HOLD",
                quantity=0,
                reason=(
                    "TOUCHED PROFIT STOP CONFIRMING — "
                    "dedicated BID timestamp unavailable"
                ),
                urgency="NORMAL",
                pnl_pct=_float(getattr(decision, "pnl_pct", 0.0)),
                reason_code="TOUCHED_PROFIT_STOP_CONFIRMING",
            )

        # The first positive floor breach starts the timer, but the existing
        # distinct-BID confirmation response remains the externally visible
        # contract until the required observations arrive.
        if _float(getattr(decision, "pnl_pct", 0.0)) > 0.0:
            _prepare_pullback_recovery(pos, decision, now_et)

        prior_key = str(getattr(pos, _STATE_KEY, "") or "")
        count = int(getattr(pos, _STATE_COUNT, 0) or 0)

        if observation_key != prior_key:
            count += 1
            setattr(pos, _STATE_COUNT, count)
            setattr(pos, _STATE_KEY, observation_key)
            if count == 1:
                setattr(pos, _STATE_STARTED, datetime.now(timezone.utc))

        required = _required_confirmations()
        if count >= required:
            pullback_hold = _prepare_pullback_recovery(pos, decision, now_et)
            if pullback_hold is not None:
                log.warning(
                    "[%s] TOUCHED_PROFIT_STOP confirmed but deferred for bounded "
                    "pullback recovery | peak=%.1f%% pnl=%.1f%%",
                    getattr(pos, "ticker", ""),
                    max(
                        _float(getattr(pos, "peak_pnl_pct", 0.0)),
                        _float(getattr(pos, "max_profit_seen", 0.0)),
                    ) * 100,
                    _float(getattr(decision, "pnl_pct", 0.0)) * 100,
                )
                return pullback_hold
            log.warning(
                "[%s] TOUCHED_PROFIT_STOP_CONFIRMED observations=%s/%s "
                "bid=%.4f pnl=%.1f%%",
                getattr(pos, "ticker", ""),
                count,
                required,
                _float(getattr(pos, "current_bid", 0.0)),
                _float(getattr(decision, "pnl_pct", 0.0)) * 100,
            )
            _reset_state(pos)
            return decision

        pnl_pct = _float(getattr(decision, "pnl_pct", 0.0))
        peak_pct = max(
            _float(getattr(pos, "peak_pnl_pct", 0.0)),
            _float(getattr(pos, "max_profit_seen", 0.0)),
        )
        quote_ts = _normalize_ts(
            getattr(
                pos,
                "last_option_bid_update_ts",
                getattr(pos, "lastoptionbidupdatets", None),
            )
        )
        log.warning(
            "[%s] TOUCHED_PROFIT_STOP_CONFIRMING observations=%s/%s "
            "peak=%.1f%% pnl=%.1f%% bid=%.4f quote_ts=%s",
            getattr(pos, "ticker", ""),
            count,
            required,
            peak_pct * 100,
            pnl_pct * 100,
            _float(getattr(pos, "current_bid", 0.0)),
            quote_ts or "missing",
        )
        return exit_decision_cls(
            action="HOLD",
            quantity=0,
            reason=(
                "TOUCHED PROFIT STOP CONFIRMING — "
                f"observation {count}/{required}; "
                f"peak +{peak_pct*100:.1f}%, current {pnl_pct*100:.1f}% "
                "on executable BID; waiting for a distinct confirming BID"
            ),
            urgency="NORMAL",
            pnl_pct=pnl_pct,
            reason_code="TOUCHED_PROFIT_STOP_CONFIRMING",
        )

    return guarded


def startup_policy_diagnostic() -> dict[str, Any]:
    diagnostic = {
        "enabled": _enabled(),
        "required_distinct_bid_observations": _required_confirmations(),
        "scope": "non_paper_touched_profit_stop_only",
        "hard_exits_unchanged": True,
        "runner_trails_unchanged": True,
    }
    log.warning("TOUCHED_PROFIT_CONFIRMATION_POLICY %s", diagnostic)
    return diagnostic


def install_touched_profit_confirmation_guard() -> None:
    """Install the idempotent guard on ``ap_exit_engine.evaluate_exit``."""
    import ap_exit_engine as engine_module

    startup_policy_diagnostic()
    if getattr(engine_module, _PATCHED_ATTR, False):
        return

    original = engine_module.evaluate_exit
    setattr(engine_module, _ORIGINAL_ATTR, original)
    engine_module.evaluate_exit = wrap_evaluate_exit(
        original,
        exit_decision_cls=engine_module.ExitDecision,
        classify_decision=engine_module._classify_exit_decision,
    )
    setattr(engine_module, _PATCHED_ATTR, True)
