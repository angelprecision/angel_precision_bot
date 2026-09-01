from __future__ import annotations

import logging
from typing import Any


log = logging.getLogger("ap.preopen_safe_partial_guard")
_INSTALLED = False

_SAFE_RESULT_CLASS = "RETRY_EXHAUSTED"
_SAFE_LAST_ERROR = "OVERNIGHT_REEVAL_RETRY_EXHAUSTED"
_SAFE_RETRY_REASON = "retryable_rows_remain"
_RETRYABLE_SOURCES = {"trade_queue", "ap_signals"}


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


def _retryable_row_identities(
    details: dict,
    expected_count: int,
) -> tuple[list[tuple[str, str, str]] | None, str | None]:
    """Load the source-row identities needed for a safe partial proof."""
    raw_rows = details.get("retryable_rows")
    if not isinstance(raw_rows, list) or len(raw_rows) != expected_count:
        return None, "retryable_row_identity_count_mismatch"

    identities: list[tuple[str, str, str]] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict):
            return None, "retryable_row_identity_invalid"
        source = _normalize_mode(raw_row.get("source"))
        job_id = str(raw_row.get("job_id") or "").strip()
        signal_id = str(raw_row.get("signal_id") or "").strip()
        if source not in _RETRYABLE_SOURCES or not job_id or not signal_id:
            return None, "retryable_row_identity_invalid"
        identities.append((source, job_id, signal_id))

    if len(set(identities)) != expected_count:
        return None, "retryable_row_identity_duplicate"
    if len({signal_id for _, _, signal_id in identities}) != expected_count:
        return None, "retryable_row_signal_identity_duplicate"
    return identities, None


def _pending_trigger_identity(row: dict) -> tuple[str, str, str] | None:
    """Read the exact overnight source identity persisted on an order."""
    if not isinstance(row, dict):
        return None
    source = _normalize_mode(row.get("overnight_source_table"))
    job_id = str(row.get("overnight_source_job_id") or "").strip()
    source_signal_id = str(row.get("overnight_source_signal_id") or "").strip()
    order_signal_id = str(row.get("signal_id") or "").strip()
    if (
        source not in _RETRYABLE_SOURCES
        or not job_id
        or not source_signal_id
        or not order_signal_id
        or source_signal_id != order_signal_id
    ):
        return None
    return source, job_id, source_signal_id


def _entry_watcher_is_live(runner) -> bool:
    """Require the canonical watcher thread, not only retained pending state."""
    watcher = getattr(getattr(runner, "core", None), "entry_watcher", None)
    if watcher is None or getattr(watcher, "_running", None) is not True:
        return False
    thread = getattr(watcher, "_thread", None)
    is_alive = getattr(thread, "is_alive", None)
    if not callable(is_alive):
        return False
    try:
        return bool(is_alive())
    except Exception:
        return False


def _latest_exact_overnight_row(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
) -> dict | None:
    from ap.morning_handoff import _latest_handoff_rows

    try:
        rows = _latest_handoff_rows(trading_date) or []
    except Exception as exc:
        log.warning(
            "SAFE_PARTIAL_HANDOFF_LOOKUP_UNAVAILABLE client_id=%s mode=%s date=%s error=%s",
            client_id,
            execution_mode,
            trading_date,
            exc,
        )
        return None

    for row in rows:
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

    # A safe partial is permitted only after both source inventories are known
    # complete. Missing or contradictory source truth is indistinguishable from
    # an incomplete inventory and must remain blocked.
    if details.get("source_lookup_partial") is not False:
        return False, {"reason": "overnight_source_lookup_partial"}
    if details.get("trade_queue_status") != "SUCCESS":
        return False, {"reason": "overnight_trade_queue_source_not_success"}
    if details.get("ap_signals_status") != "SUCCESS":
        return False, {"reason": "overnight_ap_signals_source_not_success"}

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
            "armed",
            "terminal_rejected",
            "already_resolved",
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
    outcome_total = sum(
        counts[key]
        for key in (
            "armed",
            "terminal_rejected",
            "terminal_errors",
            "retryable_deferred",
            "already_resolved",
            "unresolved",
        )
    )
    if outcome_total != counts["fetched"]:
        return False, {"reason": "overnight_outcome_accounting_mismatch"}

    current_attempt_count = getattr(runner, "_overnight_reeval_attempt_count", None)
    current_attempt_at = getattr(runner, "_overnight_reeval_last_attempt_at", None)
    try:
        current_attempted_at = current_attempt_at.isoformat()
    except Exception:
        current_attempted_at = ""
    if (
        isinstance(current_attempt_count, bool)
        or not isinstance(current_attempt_count, int)
        or current_attempt_count != counts["attempt_count"]
        or not current_attempted_at
        or str(details.get("attempted_at") or "").strip() != current_attempted_at
    ):
        return False, {"reason": "overnight_lock_not_current_attempt"}

    if client_state.get("stale_processing_ids"):
        return False, {"reason": "stale_processing_rows_present"}
    if client_state.get("watching_orphans"):
        return False, {"reason": "watching_orphans_present"}

    pending_rows = client_state.get("pending_trigger_rows") or []
    if len(pending_rows) < counts["retryable_deferred"]:
        return False, {"reason": "retryable_rows_not_durably_represented"}

    retryable_rows, identity_error = _retryable_row_identities(
        details,
        counts["retryable_deferred"],
    )
    if identity_error:
        return False, {"reason": identity_error}

    pending_by_identity: dict[tuple[str, str, str], list[dict]] = {}
    for pending_row in pending_rows:
        if not isinstance(pending_row, dict):
            return False, {"reason": "pending_trigger_row_invalid"}
        identity = _pending_trigger_identity(pending_row)
        if identity is not None:
            pending_by_identity.setdefault(identity, []).append(pending_row)
    for source, job_id, signal_id in retryable_rows or []:
        identity = (source, job_id, signal_id)
        matches = pending_by_identity.get(identity) or []
        if len(matches) != 1 or not str(matches[0].get("local_order_id") or "").strip():
            return False, {
                "reason": "retryable_rows_not_exactly_represented",
                "source": source,
                "job_id": job_id,
                "signal_id": signal_id,
            }

    if not _entry_watcher_is_live(runner):
        return False, {"reason": "entry_watcher_not_live"}

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
        "retryable_rows": retryable_rows,
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
