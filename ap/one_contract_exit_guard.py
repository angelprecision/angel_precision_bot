"""Preserve the intended LIVE one-contract runner precedence.

``evaluate_exit`` documents that a one-contract position must skip scale-out and
run until the runner trail. The earlier generic touched-profit floor currently
preempts that path: a contract that peaks +5% can be closed at +3% before the
existing runner-arm threshold is reached.

This guard suppresses only that contradictory early-green decision. It does not
suppress hard/underlying stops, EOD, target exits, loss exits, or any touched-
profit exit once the option is at/below breakeven. No broker submit/cancel code
is changed.

CANONICAL THRESHOLD POLICY
───────────────────────────
The runner-arm threshold is the same value the exit engine already uses as
``immediate_tp`` via ``ap_exit_engine._effective_thresholds(pos)``:

    0DTE index (SPY/QQQ/IWM/SPX/…): +20%
    0DTE equity:                     +22%
    DTE ≤ 2:                         +25%
    standard:                        +12%

There is exactly ONE source of truth.  This guard must never introduce a
second configured threshold that could diverge from the engine.
"""
from __future__ import annotations

import os
from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.one_contract_exit_guard")

_PATCHED_ATTR   = "_AP_ONE_CONTRACT_EXIT_GUARD_PATCHED"
_ORIGINAL_ATTR  = "_AP_ONE_CONTRACT_EXIT_GUARD_ORIGINAL"


# ── Feature flag ─────────────────────────────────────────────────────────────

def _enabled() -> bool:
    """Require an explicit rollout decision for this LIVE exit-policy change."""
    return str(
        os.getenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "0")
    ).strip().lower() in {"1", "true", "yes", "on"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _resolve_canonical_runner_arm(pos: Any, engine_module: Any) -> float:
    """Return the canonical runner-arm threshold from the exit engine.

    Calls ``engine_module._effective_thresholds(pos)`` which returns
    ``(hard_stop, immediate_tp, profit_lock)``.  The ``immediate_tp`` value IS
    the runner-arm threshold — it's the point at which the exit engine switches
    from the touched-profit-stop path to the runner trail.

    Falls back to 0.12 (the standard-DTE value) only if the engine call fails,
    to avoid a guard crash breaking live exit decisions.
    """
    try:
        _hard_stop, runner_arm_pct, _profit_lock = engine_module._effective_thresholds(pos)
        return float(runner_arm_pct)
    except Exception as exc:
        log.warning(
            "one_contract_exit_guard: _effective_thresholds failed (%s) — "
            "falling back to 0.12 standard threshold",
            exc,
        )
        return 0.12


# ── Core predicate ────────────────────────────────────────────────────────────

def should_hold_early_green_one_contract(
    pos: Any,
    decision: Any,
    *,
    decision_code: str,
    runner_arm_pct: float,
) -> bool:
    """Return True only for the contradictory pre-runner LIVE profit-floor exit.

    ALL conditions must hold simultaneously:
      • LIVE execution mode (authoritative)
      • Exactly one contract remaining
      • No completed scale-out (scale_outs_done == 0)
      • Decision code is exactly TOUCHED_PROFIT_STOP
      • Action is exactly CLOSE_ALL
      • Current option P&L is strictly positive
      • Peak P&L is strictly positive AND strictly below the canonical runner-arm

    ``runner_arm_pct`` must come from ``_effective_thresholds(pos)`` via
    ``_resolve_canonical_runner_arm()`` — the caller is responsible for this.
    """
    mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
    qty_remaining   = _int(getattr(pos, "quantity_remaining", 0))
    scale_outs_done = _int(getattr(pos, "scale_outs_done", 0))
    current_pnl     = _float(getattr(pos, "option_pnl_pct", 0.0))
    peak_pnl = max(
        _float(getattr(pos, "peak_pnl_pct", 0.0)),
        _float(getattr(pos, "max_profit_seen", 0.0)),
    )

    return bool(
        mode == "live"
        and qty_remaining == 1
        and scale_outs_done == 0
        and str(decision_code or "").strip().upper() == "TOUCHED_PROFIT_STOP"
        and str(getattr(decision, "action", "") or "").strip().upper() == "CLOSE_ALL"
        and current_pnl > 0.0
        and 0.0 < peak_pnl < float(runner_arm_pct)
    )


# ── Startup diagnostic ────────────────────────────────────────────────────────

def startup_policy_diagnostic() -> dict[str, Any]:
    """Log the guard state at startup.

    Reports exactly two things:
      1. Whether the feature flag is enabled.
      2. A warning that enabling changes LIVE exit timing.

    Does NOT log a separate configured threshold.  The canonical threshold
    is resolved per-position at runtime via ``_effective_thresholds(pos)``.
    Logging a single number here would imply a fixed policy when the actual
    policy is DTE/instrument-dependent.
    """
    enabled = _enabled()
    log.warning(
        "LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE flag=%s "
        "canonical_threshold=DTE_and_instrument_dependent "
        "live_exit_timing_changes_when_enabled=true",
        "enabled" if enabled else "disabled",
    )
    return {
        "enabled": enabled,
        "canonical_threshold_policy": {
            "0DTE_index":  0.20,
            "0DTE_equity": 0.22,
            "DTE_lte_2":   0.25,
            "standard":    0.12,
        },
        "changes_live_exit_timing_when_enabled": True,
    }


# ── Wrapper ───────────────────────────────────────────────────────────────────

def wrap_evaluate_exit(
    original: Callable[..., Any],
    *,
    exit_decision_cls: type,
    classify_decision: Callable[[Any], str],
    engine_module: Any,
) -> Callable[..., Any]:
    """Wrap ``evaluate_exit`` with the one-contract runner-precedence guard.

    When the feature flag is disabled, the original decision is returned
    unchanged — identical object, zero overhead.

    When enabled, only the exact TOUCHED_PROFIT_STOP / CLOSE_ALL shape on a
    single-contract LIVE position below the canonical runner-arm threshold is
    suppressed.  Everything else passes through unchanged.
    """
    def guarded(pos, now_et=None):
        decision = original(pos, now_et=now_et)

        # Fast path: flag off → original decision, unchanged.
        if not _enabled():
            return decision

        # Resolve classification.
        try:
            decision_code = classify_decision(decision)
        except Exception:
            decision_code = str(getattr(decision, "reason_code", "") or "")

        # Canonical runner-arm threshold — single source of truth.
        runner_arm_pct = _resolve_canonical_runner_arm(pos, engine_module)

        if not should_hold_early_green_one_contract(
            pos,
            decision,
            decision_code=decision_code,
            runner_arm_pct=runner_arm_pct,
        ):
            return decision

        peak_pnl    = max(
            _float(getattr(pos, "peak_pnl_pct", 0.0)),
            _float(getattr(pos, "max_profit_seen", 0.0)),
        )
        current_pnl = _float(getattr(pos, "option_pnl_pct", 0.0))
        log.info(
            "[%s] SINGLE_CONTRACT_EARLY_GREEN_HOLD "
            "peak=%.1f%% current=%.1f%% runner_arm=%.1f%% suppressed=%s",
            getattr(pos, "ticker", ""),
            peak_pnl    * 100,
            current_pnl * 100,
            runner_arm_pct * 100,
            decision_code,
        )
        return exit_decision_cls(
            action="HOLD",
            quantity=0,
            reason=(
                f"SINGLE CONTRACT EARLY GREEN HOLD — "
                f"peaked +{peak_pnl*100:.1f}% "
                f"now +{current_pnl*100:.1f}% — "
                f"waiting for +{runner_arm_pct*100:.0f}% runner arm; "
                f"suppressed={decision_code}"
            ),
            urgency="NORMAL",
            pnl_pct=current_pnl,
            reason_code="SINGLE_CONTRACT_EARLY_GREEN_HOLD",
        )

    return guarded


# ── Installer ─────────────────────────────────────────────────────────────────

def install_one_contract_exit_guard() -> None:
    """Install the guard onto ``ap_exit_engine.evaluate_exit`` at startup.

    Idempotent — a second call is a no-op.

    The feature flag defaults to off.  Enabling it changes LIVE exit timing.
    """
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
        engine_module=engine_module,
    )
    setattr(engine_module, _PATCHED_ATTR, True)
