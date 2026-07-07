"""P0 guard: broker-truth repair exits must reduce risk truthfully.

Two incident shapes are handled here:

1. Broker truth says open qty > 0, but the rejection circuit breaker blocks the
   protective exit.  In that case the circuit breaker is bypassed only for the
   broker-truth repair context.
2. Broker truth says open qty == 0 for a broker-repair/synthetic position.  In
   that case the engine must stop repeatedly firing exits against a stale repair
   row.  The safety result is rewritten to a terminal stale/flat reason so the
   caller can clear or close in-memory state instead of looping forever.

Normal positions are not loosened.
"""
from __future__ import annotations

from typing import Any

from ap.logger import get_logger

log = get_logger("ap.exit_circuit_breaker_broker_truth_guard")

_PATCHED_ATTR = "_AP_BROKER_TRUTH_EXIT_BREAKER_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_BROKER_TRUTH_EXIT_BREAKER_GUARD_ORIGINAL"
FLAT_REASON = "broker_truth_position_flat"


def _nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except Exception:
        return 0


def _is_broker_truth_repair_context(kwargs: dict[str, Any]) -> bool:
    position_id = str(kwargs.get("position_id") or "")
    return bool(
        kwargs.get("allow_missing_position_with_broker_truth")
        or position_id.startswith("broker-repair-")
    )


def apply_broker_truth_exit_breaker_bypass(result: Any, kwargs: dict[str, Any]) -> Any:
    """Pure post-processor for evaluate_exit_submission_safety results.

    - repair context + broker qty > 0 + circuit breaker -> allow protective exit
    - repair context + broker qty == 0 -> block as broker_truth_position_flat
    """
    if not isinstance(result, dict):
        return result
    broker_qty = _nonnegative_int(kwargs.get("broker_truth_open_qty"))
    if not _is_broker_truth_repair_context(kwargs):
        return result

    if broker_qty <= 0:
        patched = dict(result)
        patched["blocked"] = True
        patched["reason"] = FLAT_REASON
        patched["p0_broker_truth_position_flat"] = True
        patched["p0_broker_truth_open_qty"] = broker_qty
        log.critical(
            "P0_BROKER_TRUTH_POSITION_FLAT position_id=%s client_id=%s execution_mode=%s contract=%s broker_truth_open_qty=%s original_reason=%s",
            str(kwargs.get("position_id") or ""),
            str(kwargs.get("client_id") or ""),
            str(kwargs.get("execution_mode") or ""),
            str(kwargs.get("contract") or ""),
            broker_qty,
            str(result.get("reason") or ""),
        )
        return patched

    reason = str(result.get("reason") or "")
    if reason == "exit_circuit_breaker_tripped":
        patched = dict(result)
        patched["blocked"] = False
        patched["reason"] = None
        patched["p0_broker_truth_circuit_breaker_bypass"] = True
        patched["p0_broker_truth_open_qty"] = broker_qty
        log.critical(
            "P0_BROKER_TRUTH_EXIT_BREAKER_BYPASS position_id=%s client_id=%s execution_mode=%s contract=%s broker_truth_open_qty=%s original_reason=%s",
            str(kwargs.get("position_id") or ""),
            str(kwargs.get("client_id") or ""),
            str(kwargs.get("execution_mode") or ""),
            str(kwargs.get("contract") or ""),
            broker_qty,
            reason,
        )
        return patched
    return result


def install_exit_circuit_breaker_broker_truth_guard() -> None:
    from ap import exit_safety

    if getattr(exit_safety, _PATCHED_ATTR, False):
        return

    original = exit_safety.evaluate_exit_submission_safety
    setattr(exit_safety, _ORIGINAL_ATTR, original)

    def guarded_evaluate_exit_submission_safety(*args, **kwargs):
        result = original(*args, **kwargs)
        try:
            return apply_broker_truth_exit_breaker_bypass(result, kwargs)
        except Exception as exc:
            log.warning("broker-truth exit breaker guard post-process failed: %s", exc)
            return result

    exit_safety.evaluate_exit_submission_safety = guarded_evaluate_exit_submission_safety
    setattr(exit_safety, _PATCHED_ATTR, True)
