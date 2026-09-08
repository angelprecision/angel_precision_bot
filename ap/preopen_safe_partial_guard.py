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
    if type(raw) is not int:
        return None
    return raw if raw >= 0 else None


def _strict_positive_int(value: Any) -> int | None:
    if type(value) is not int or value <= 0:
        return None
    return value


def _pending_generation(value: Any) -> int | None:
    """Normalize the text returned by PostgreSQL meta->> generation fields."""
    if isinstance(value, bool):
        return None
    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed > 0 else None
    return None


def _retryable_row_identities(
    details: dict,
    expected_count: int,
    *,
    client_id: str,
    execution_mode: str,
    attempt_id: str,
    attempt_generation: int,
    session_key: str,
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
        canonical = str(raw_row.get("canonical_signal_id") or "").strip()
        row_client = str(raw_row.get("client_id") or "").strip()
        row_mode = _normalize_mode(raw_row.get("execution_mode"))
        row_session = str(
            raw_row.get("overnight_reeval_session_key") or ""
        ).strip()
        row_attempt_id = str(raw_row.get("attempt_id") or "").strip()
        row_generation = raw_row.get("attempt_generation")
        if (
            source not in _RETRYABLE_SOURCES
            or not job_id
            or not signal_id
            or not canonical
            or row_client != client_id
            or row_mode != execution_mode
            or row_session != session_key
            or row_attempt_id != attempt_id
            or row_generation != attempt_generation
        ):
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


def _pending_trigger_authority_matches(
    row: dict,
    *,
    client_id: str,
    execution_mode: str,
    attempt_id: str,
    attempt_generation: int,
    session_key: str,
) -> bool:
    if not isinstance(row, dict):
        return False
    if str(row.get("client_id") or "").strip() != client_id:
        return False
    if str(row.get("meta_client_id") or "").strip() != client_id:
        return False
    if _normalize_mode(row.get("execution_mode")) != execution_mode:
        return False
    if _normalize_mode(row.get("meta_execution_mode")) != execution_mode:
        return False
    canonical = str(row.get("canonical_signal_id") or "").strip()
    meta_canonical = str(row.get("meta_canonical_signal_id") or "").strip()
    if not canonical or not meta_canonical or canonical != meta_canonical:
        return False
    if str(row.get("overnight_reeval_session_key") or "").strip() != session_key:
        return False
    if str(row.get("attempt_id") or "").strip() != attempt_id:
        return False
    if _pending_generation(row.get("attempt_generation")) != attempt_generation:
        return False
    return True


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
    """Prove exact row ownership for an exhausted overnight partial.

    This is an individual-row ownership proof only.  It is deliberately not
    an account-wide overnight success certificate: the latest durable attempt
    remains authoritative for readiness, and any partial/retryable attempt
    keeps readiness blocked even when every remaining row is safely owned.
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

    # A durable source-authority contradiction is never eligible for the
    # #559 safe-partial exception, even if a future caller accidentally labels
    # the lock RETRY_EXHAUSTED. The conflict diagnostics must remain a hard
    # readiness boundary, not become an exhausted-retry certificate.
    if details.get("source_identity_conflict") or details.get("source_identity_conflicts"):
        return False, {"reason": "overnight_source_identity_conflict"}

    # A safe partial is permitted only after both source inventories are known
    # complete. Missing or contradictory source truth is indistinguishable from
    # an incomplete inventory and must remain blocked.
    if details.get("source_lookup_partial") is not False:
        return False, {"reason": "overnight_source_lookup_partial"}
    if details.get("trade_queue_status") != "SUCCESS":
        return False, {"reason": "overnight_trade_queue_source_not_success"}
    if details.get("ap_signals_status") != "SUCCESS":
        return False, {"reason": "overnight_ap_signals_source_not_success"}

    expected_client_id = str(client_id or "").strip()
    expected_mode = _normalize_mode(execution_mode)
    expected_attempt_id = str(details.get("attempt_id") or "").strip()
    expected_generation = _strict_positive_int(details.get("attempt_generation"))
    expected_session = str(
        details.get("overnight_reeval_session_key") or ""
    ).strip()
    if (
        not expected_attempt_id
        or expected_generation is None
        or not expected_session
        or str(details.get("client_id") or "").strip() != expected_client_id
        or _normalize_mode(details.get("execution_mode")) != expected_mode
        or str(details.get("trading_date") or "").strip()[:10] != str(trading_date)[:10]
    ):
        return False, {"reason": "overnight_attempt_authority_invalid"}

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
    if (
        getattr(runner, "_overnight_reeval_attempt_id", None) != expected_attempt_id
        or getattr(runner, "_overnight_reeval_attempt_generation", None) != expected_generation
        or _normalize_mode(getattr(runner, "mode", None)) != expected_mode
        or str(getattr(runner, "email", "") or "").strip() != expected_client_id
    ):
        return False, {"reason": "overnight_attempt_not_owned_by_runner"}

    accounting = details.get("source_row_accounting")
    if not isinstance(accounting, dict):
        return False, {"reason": "overnight_source_accounting_invalid"}
    accounting_keys = (
        "raw_source_rows_fetched",
        "raw_trade_queue_rows",
        "raw_ap_signals_rows",
        "equivalent_duplicate_rows_collapsed",
        "logical_rows_before_setup_dedup",
        "logical_rows_after_setup_dedup",
        "setup_duplicate_rows_collapsed",
        "source_identity_conflict_rows",
    )
    accounting_values = {}
    for key in accounting_keys:
        value = accounting.get(key)
        if type(value) is not int or value < 0:
            return False, {"reason": "overnight_source_accounting_invalid"}
        accounting_values[key] = value
    if accounting_values["source_identity_conflict_rows"] != 0:
        return False, {"reason": "overnight_source_identity_conflict"}
    if accounting_values["logical_rows_after_setup_dedup"] != counts["fetched"]:
        return False, {"reason": "overnight_fetched_accounting_mismatch"}
    if (
        accounting_values["logical_rows_before_setup_dedup"]
        != accounting_values["logical_rows_after_setup_dedup"]
        + accounting_values["setup_duplicate_rows_collapsed"]
    ):
        return False, {"reason": "overnight_setup_dedup_accounting_mismatch"}
    if accounting_values["raw_source_rows_fetched"] < accounting_values["logical_rows_before_setup_dedup"]:
        return False, {"reason": "overnight_raw_source_accounting_mismatch"}

    if client_state.get("stale_processing_ids"):
        return False, {"reason": "stale_processing_rows_present"}
    if client_state.get("watching_orphans"):
        return False, {"reason": "watching_orphans_present"}

    pending_rows = client_state.get("pending_trigger_rows") or []
    if len(pending_rows) != counts["retryable_deferred"]:
        return False, {"reason": "retryable_rows_not_durably_represented"}

    retryable_rows, identity_error = _retryable_row_identities(
        details,
        counts["retryable_deferred"],
        client_id=expected_client_id,
        execution_mode=expected_mode,
        attempt_id=expected_attempt_id,
        attempt_generation=expected_generation,
        session_key=expected_session,
    )
    if identity_error:
        return False, {"reason": identity_error}

    expected_identities = set(retryable_rows or [])
    pending_identities: list[tuple[str, str, str]] = []
    for pending_row in pending_rows:
        if not isinstance(pending_row, dict):
            return False, {"reason": "pending_trigger_row_invalid"}
        identity = _pending_trigger_identity(pending_row)
        if identity is None:
            return False, {"reason": "pending_trigger_identity_invalid"}
        pending_identities.append(identity)

    for identity in retryable_rows or []:
        if identity not in pending_identities:
            return False, {
                "reason": "retryable_rows_not_exactly_represented",
                "source": identity[0],
                "job_id": identity[1],
                "signal_id": identity[2],
            }
    for identity in pending_identities:
        if identity not in expected_identities:
            return False, {
                "reason": "retryable_rows_not_exactly_represented",
                "source": identity[0],
                "job_id": identity[1],
                "signal_id": identity[2],
            }

    pending_by_identity: dict[tuple[str, str, str], list[dict]] = {}
    for pending_row, identity in zip(pending_rows, pending_identities):
        if _normalize_mode(pending_row.get("execution_mode")) != expected_mode:
            return False, {"reason": "pending_trigger_execution_mode_mismatch"}
        if not _pending_trigger_authority_matches(
            pending_row,
            client_id=expected_client_id,
            execution_mode=expected_mode,
            attempt_id=expected_attempt_id,
            attempt_generation=expected_generation,
            session_key=expected_session,
        ):
            return False, {"reason": "pending_trigger_authority_invalid"}
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
        "attempt_id": expected_attempt_id,
        "attempt_generation": expected_generation,
        "client_id": expected_client_id,
        "execution_mode": expected_mode,
        "overnight_reeval_session_key": expected_session,
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

        # Keep the exact row-ownership proof available for diagnostics, but do
        # not promote it to a readiness state.  A partial durable attempt is
        # still incomplete account-wide truth, and the latest exact durable
        # attempt must remain the sole readiness authority for every mode.
        blocked_details = dict(details or {})
        blocked_details["safe_partial_row_proof"] = proof
        blocked_details["safe_partial_account_readiness"] = "blocked_latest_attempt_incomplete"
        log.warning(
            "PREOPEN_SAFE_PARTIAL_ROW_OWNERSHIP_PROVEN_ACCOUNT_READINESS_BLOCKED "
            "client_id=%s mode=%s date=%s retryable_deferred=%s attempt_count=%s",
            client_id,
            execution_mode,
            trading_date,
            proof.get("retryable_deferred"),
            proof.get("attempt_count"),
        )
        return "missing", blocked_details

    _wrapped_overnight_status._ap_safe_partial_guard = True
    base._overnight_status = _wrapped_overnight_status
    _INSTALLED = True
