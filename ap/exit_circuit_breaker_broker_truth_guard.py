"""P0 guard: broker-truth-open protective exits must not be trapped by the
exit rejection circuit breaker.

Incident shape: synthetic repair / broker-truth position still has open quantity,
but repeated prior reject/error rows make evaluate_exit_submission_safety return
exit_circuit_breaker_tripped. That is safe for normal repeated bad submits, but
unsafe for a protective CLOSE_ALL when broker truth says qty > 0: the system keeps
wanting to exit and the breaker keeps blocking risk reduction.

This guard is intentionally narrow. It only bypasses the circuit breaker when the
call already opted into broker-truth handling via
allow_missing_position_with_broker_truth or a broker-repair-* position id, and
broker_truth_open_qty is positive. It does not loosen normal exit circuit-breaker
behavior.
"""
from __future__ import annotations

from typing import Any

from ap.logger import get_logger

log = get_logger("ap.exit_circuit_breaker_broker_truth_guard")

_PATCHED_ATTR = "_AP_BROKER_TRUTH_EXIT_BREAKER_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_BROKER_TRUTH_EXIT_BREAKER_GUARD_ORIGINAL"


def _positive_int(value: Any) -> int:
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


def install_exit_circuit_breaker_broker_truth_guard() -> None:
    from ap import exit_safety

    if getattr(exit_safety, _PATCHED_ATTR, False):
        return

    original = exit_safety.evaluate_exit_submission_safety
    setattr(exit_safety, _ORIGINAL_ATTR, original)

    def guarded_evaluate_exit_submission_safety(*args, **kwargs):
        result = original(*args, **kwargs)
        try:
            if not isinstance(result, dict):
                return result
            reason = str(result.get("reason") or "")
            broker_qty = _positive_int(kwargs.get("broker_truth_open_qty"))
            if (
                reason == "exit_circuit_breaker_tripped"
                and broker_qty > 0
                and _is_broker_truth_repair_context(kwargs)
            ):
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
        except Exception as exc:
            log.warning("broker-truth exit breaker guard post-process failed: %s", exc)
        return result

    exit_safety.evaluate_exit_submission_safety = guarded_evaluate_exit_submission_safety
    setattr(exit_safety, _PATCHED_ATTR, True)
