from __future__ import annotations

import logging
from typing import Any


log = logging.getLogger("ap.preopen_safe_partial_guard")
_INSTALLED = False

_SAFE_RESULT_CLASS = "RETRY_EXHAUSTED"
_SAFE_LAST_ERROR = "OVERNIGHT_REEVAL_RETRY_EXHAUSTED"
_SAFE_RETRY_REASON = "retryable_rows_remain"


def _normalize_mode(value: Any) -> str:
    return str(value or "").strip().lower()


def _strict_nonnegative_int(details: dict, key: str) -> int | None:
    raw = details.get(key)
    if isinstance(raw, bool) or isinstance(raw, float):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None


def _latest_exact_overnight_row(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
) -> dict | None:
    from ap.morning_handoff import _latest_handoff_rows

    for row in _latest_handoff_rows(trading_date) or []:
        if str(row.get("client_id") or "").strip() != client_id:
            continue
        if _normalize_mode(row.get("execution_mode")) != execution_mode:
            continue
        if str(row.get("stage") or "").strip().lower() != "overnight_reeval":
            continue
        return row
    return None


def classify_safe_exhausted_partial(
    base_module,
    *,
    runner,
    client_state: dict,
    client_id: str,
    execution_mode: str,
    trading_date: str,
) -> tuple[bool, dict]:
    """Prove an exhausted overnight partial is safe for account readiness.

    This does not declare every selector retry successful. It only proves that
    the account-wide ownership/readiness invariant is satisfied while the
    remaining per-signal retryable rows stay isolated under their own durable
    watcher ownership.
    """
    row = _latest_exact_overnight_row(
        client_id=client_id,
        execution_mode=execution_mode,
        trading_date=trading_date,
    )
    if not row:
        return False, {"reason": "overnight_lock_missing"}

    if str(row.get("status") or "").strip().lower() != "partial":
        return False, {"reason": "overnight_lock_not_partial"}
    if str(row.get("last_error") or "").strip() != _SAFE_LAST_ERROR:
        return False, {"reason": "overnight_lock_not_bounded_retry_exhaustion"}

    details = row.get("details") or {}
    if not isinstance(details, dict):
        return False, {"reason": "overnight_details_invalid"}

    if str(details.get("result_class") or "").strip().upper() != _SAFE_RESULT_CLASS:
        return False, {"reason": "overnight_result_class_not_retry_exhausted"}
    if str(details.get("retry_reason") or "").strip() != _SAFE_RETRY_REASON:
        return False, {"reason": "overnight_retry_reason_not_retryable_rows_remain"}
    if details.get("completed") is not False or details.get("retryable") is not False:
        return False, {"reason": "overnight_terminal_flags_invalid"}

    counts = {
        key: _strict_nonnegative_int(details, key)
        for key in (
            "fetched",
            "processed",
            "fresh_processed",
            "errors",
            "terminal_errors",
            "unresolved",
            "retryable_deferred",
            "attempt_count",
        )
    }
    if any(value is None for value in counts.values()):
        return False, {"reason": "overnight_counts_invalid"}
    if counts["fetched"] <= 0:
        return False, {"reason": "overnight_fetched_not_positive"}
    if counts["processed"] != counts["fetched"]:
        return False, {"reason": "overnight_source_not_fully_processed"}
    if counts["fresh_processed"] != counts["processed"]:
        return False, {"reason": "overnight_fresh_processing_incomplete"}
    if counts["errors"] != 0 or counts["terminal_errors"] != 0 or counts["unresolved"] != 0:
        return False, {"reason": "overnight_has_unresolved_or_error_truth"}
    if counts["retryable_deferred"] <= 0 or counts["attempt_count"] <= 0:
        return False, {"reason": "overnight_retryable_partial_shape_missing"}

    if client_state.get("stale_processing_ids"):
        return False, {"reason": "stale_processing_rows_present"}
    if client_state.get("watching_orphans"):
        return False, {"reason": "watching_orphans_present"}

    pending_rows = client_state.get("pending_trigger_rows") or []
    if len(pending_rows) < counts["retryable_deferred"]:
        return False, {"reason": "retryable_rows_not_durably_represented"}

    unowned = base_module._pending_trigger_without_watcher(runner, pending_rows)
    if unowned:
        return False, {
            "reason": "pending_trigger_without_watcher_ownership",
            "unowned_count": len(unowned),
        }

    return True, {
        "source": "handoff_run_locks.overnight_reeval_safe_exhausted_partial",
        "result_class": _SAFE_RESULT_CLASS,
        "retryable_deferred": counts["retryable_deferred"],
        "attempt_count": counts["attempt_count"],
        "processed": counts["processed"],
        "fetched": counts["fetched"],
        "owned_pending_trigger_count": len(pending_rows),
    }


def install_preopen_safe_partial_guard() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from ap import preopen_readiness as base

    original = getattr(base, "_overnight_status", None)
    if original is None:
        raise RuntimeError("ap.preopen_readiness._overnight_status missing")
    if getattr(original, "_ap_safe_partial_guard", False):
        _INSTALLED = True
        return

    def _wrapped_overnight_status(
        runner,
        client_state: dict,
        trading_date: str,
        *,
        client_id: str,
        execution_mode: str,
        stage: str = "",
        now=None,
    ) -> tuple[str, dict]:
        state, details = original(
            runner,
            client_state,
            trading_date,
            client_id=client_id,
            execution_mode=execution_mode,
            stage=stage,
            now=now,
        )
        if state != "missing":
            return state, details

        safe, proof = classify_safe_exhausted_partial(
            base,
            runner=runner,
            client_state=client_state,
            client_id=str(client_id or "").strip(),
            execution_mode=_normalize_mode(execution_mode),
            trading_date=trading_date,
        )
        if not safe:
            return state, details

        log.warning(
            "PREOPEN_SAFE_PARTIAL_ACCEPTED client_id=%s mode=%s date=%s retryable_deferred=%s attempt_count=%s",
            client_id,
            execution_mode,
            trading_date,
            proof.get("retryable_deferred"),
            proof.get("attempt_count"),
        )
        return "safe_partial", proof

    _wrapped_overnight_status._ap_safe_partial_guard = True
    base._overnight_status = _wrapped_overnight_status
    _INSTALLED = True
