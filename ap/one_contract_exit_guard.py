"""Preserve the intended LIVE one-contract runner precedence.

``evaluate_exit`` documents that a one-contract position must skip scale-out and
run until the runner trail. The earlier generic touched-profit floor currently
preempts that path: a contract that peaks +5% can be closed at +3% before the
existing +12% runner-arm threshold is reached.

This guard suppresses only that contradictory early-green decision. It does not
suppress hard/underlying stops, EOD, target exits, loss exits, or any touched-
profit exit once the option is at/below breakeven. No broker submit/cancel code
is changed.
"""
from __future__ import annotations

import os
from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.one_contract_exit_guard")

_PATCHED_ATTR = "_AP_ONE_CONTRACT_EXIT_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_ONE_CONTRACT_EXIT_GUARD_ORIGINAL"


def _enabled() -> bool:
    """Require an explicit rollout decision for this LIVE exit-policy change."""
    return str(
        os.getenv("LIVE_SINGLE_CONTRACT_RUNNER_PRECEDENCE_ENABLED", "0")
    ).strip().lower() in {"1", "true", "yes", "on"}


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


def _runner_arm_pct() -> float:
    raw = _float(os.getenv("SINGLE_CONTRACT_RUNNER_ARM_PCT", "0.12"), 0.12)
    return max(0.05, min(0.30, raw))


def should_hold_early_green_one_contract(
    pos: Any,
    decision: Any,
    *,
    decision_code: str,
    runner_arm_pct: float | None = None,
) -> bool:
    """Return True only for the contradictory pre-runner LIVE profit-floor exit."""
    mode = str(getattr(pos, "execution_mode", "") or "").strip().lower()
    qty_remaining = _int(getattr(pos, "quantity_remaining", 0))
    scale_outs_done = _int(getattr(pos, "scale_outs_done", 0))
    current_pnl = _float(getattr(pos, "option_pnl_pct", 0.0))
    peak_pnl = max(
        _float(getattr(pos, "peak_pnl_pct", 0.0)),
        _float(getattr(pos, "max_profit_seen", 0.0)),
    )
    arm = _runner_arm_pct() if runner_arm_pct is None else float(runner_arm_pct)

    return bool(
        mode == "live"
        and qty_remaining == 1
        and scale_outs_done == 0
        and str(decision_code or "").strip().upper() == "TOUCHED_PROFIT_STOP"
        and peak_pnl > 0.0
        and peak_pnl < arm
        and current_pnl > 0.0
        and str(getattr(decision, "action", "") or "").strip().upper() == "CLOSE_ALL"
    )


def wrap_evaluate_exit(
    original: Callable[..., Any],
    *,
    exit_decision_cls: type,
    classify_decision: Callable[[Any], str],
) -> Callable[..., Any]:
    def guarded(pos, now_et=None):
        decision = original(pos, now_et=now_et)
        if not _enabled():
            return decision
        try:
            decision_code = classify_decision(decision)
        except Exception:
            decision_code = str(getattr(decision, "reason_code", "") or "")

        arm = _runner_arm_pct()
        if not should_hold_early_green_one_contract(
            pos,
            decision,
            decision_code=decision_code,
            runner_arm_pct=arm,
        ):
            return decision

        peak_pnl = max(
            _float(getattr(pos, "peak_pnl_pct", 0.0)),
            _float(getattr(pos, "max_profit_seen", 0.0)),
        )
        current_pnl = _float(getattr(pos, "option_pnl_pct", 0.0))
        log.info(
            "[%s] SINGLE_CONTRACT_EARLY_GREEN_HOLD peak=%.1f%% current=%.1f%% "
            "runner_arm=%.1f%% suppressed=%s",
            getattr(pos, "ticker", ""),
            peak_pnl * 100,
            current_pnl * 100,
            arm * 100,
            decision_code,
        )
        return exit_decision_cls(
            action="HOLD",
            quantity=0,
            reason=(
                f"SINGLE CONTRACT EARLY GREEN HOLD — peaked +{peak_pnl*100:.1f}% "
                f"now +{current_pnl*100:.1f}% — waiting for +{arm*100:.0f}% runner arm; "
                f"suppressed={decision_code}"
            ),
            urgency="NORMAL",
            pnl_pct=current_pnl,
            reason_code="SINGLE_CONTRACT_EARLY_GREEN_HOLD",
        )

    return guarded


def install_one_contract_exit_guard() -> None:
    import ap_exit_engine as engine_module

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
