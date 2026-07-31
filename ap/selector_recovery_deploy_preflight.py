"""Read-only deployment preflight for PR #401 selector recovery cursors.

This module detects active deferred-materialization rows that would reach
selector attempt 2+ without a valid durable selector recovery cursor.

It performs no writes, broker calls, lifecycle transitions, submissions,
cancellations, or cursor adoption.

CLI:
    python -m ap.selector_recovery_deploy_preflight

Exit codes:
    0: PASS, no unsafe rows
    2: FAIL, one or more unsafe rows
    3: ERROR, preflight could not complete
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from ap.selector_retry_policy import load_selector_recovery_cursor


ACTIVE_SELECTOR_RECOVERY_ROWS_SQL = """
SELECT
    local_order_id,
    client_id,
    execution_mode,
    signal_id,
    status,
    broker_order_id,
    submitted_ts,
    updated_ts,
    meta
FROM orders
WHERE UPPER(COALESCE(kind, '')) = 'ENTRY'
  AND UPPER(COALESCE(status, '')) = 'PENDING_TRIGGER'
  AND COALESCE(broker_order_id, '') = ''
  AND submitted_ts IS NULL
  AND LOWER(TRIM(COALESCE(execution_mode, ''))) IN ('live', 'paper')
  -- Match runtime startup-recovery compat: a row enters recovery when either
  -- lifecycle_state OR materialization_status names an active retry state.
  -- Selecting on either field prevents a false PASS from a row whose
  -- lifecycle_state is blank/missing while materialization_status is present.
  AND (
        UPPER(TRIM(COALESCE(meta->>'lifecycle_state', ''))) IN (
            'RETRY_WAIT',
            'MATERIALIZING'
          )
        OR UPPER(TRIM(COALESCE(meta->>'materialization_status', ''))) IN (
            'RETRY_PENDING',
            'RUNNING'
          )
      )
  AND NULLIF(COALESCE(meta->>'submit_intent_at', ''), '') IS NULL
  AND LOWER(COALESCE(meta->>'broker_ready', 'false'))
        NOT IN ('true', '1', 'yes')
ORDER BY updated_ts ASC, local_order_id ASC
""".strip()


def _coerce_meta(raw: Any) -> tuple[dict[str, Any], str | None]:
    if isinstance(raw, Mapping):
        return dict(raw), None

    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}, "MALFORMED_ORDER_META"

        if isinstance(parsed, Mapping):
            return dict(parsed), None
        return {}, "MALFORMED_ORDER_META"

    if raw in (None, ""):
        return {}, None

    return {}, "MALFORMED_ORDER_META"


def _parse_counter(
    meta: Mapping[str, Any],
    key: str,
    *,
    default: int,
    minimum: int = 0,
) -> tuple[int, str | None]:
    raw = meta.get(key)

    if raw in (None, ""):
        return default, None

    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default, f"INVALID_COUNTER:{key}"

    if value < minimum:
        return default, f"INVALID_COUNTER:{key}"

    return value, None


def _attempt_state(
    lifecycle_state: str,
    meta: Mapping[str, Any],
) -> tuple[int, int, str, str | None]:
    retry_attempt, error = _parse_counter(
        meta,
        "retry_attempt",
        default=0,
    )
    if error:
        return 0, 0, "retry_attempt", error

    if lifecycle_state == "RETRY_WAIT":
        # Startup recovery consumes retry_attempt + 1.
        return (
            retry_attempt,
            retry_attempt + 1,
            "retry_attempt+1",
            None,
        )

    # Match execution-core precedence for an already MATERIALIZING row.
    if retry_attempt > 0:
        return retry_attempt, retry_attempt, "retry_attempt", None

    for key in (
        "materialization_attempts",
        "materialization_current_attempt",
    ):
        value, error = _parse_counter(meta, key, default=0)
        if error:
            return 0, 0, key, error
        if value > 0:
            return value, value, key, None

    return 1, 1, "default_first_attempt", None


def _generation_state(
    lifecycle_state: str,
    meta: Mapping[str, Any],
) -> tuple[int, int, str | None]:
    # Startup recovery treats a missing RETRY_WAIT generation as generation 1.
    missing_default = 1 if lifecycle_state == "RETRY_WAIT" else 0
    persisted_generation, error = _parse_counter(
        meta,
        "materialization_generation",
        default=missing_default,
        minimum=0,
    )
    if error:
        return persisted_generation, persisted_generation, error

    if persisted_generation < 1:
        return (
            persisted_generation,
            persisted_generation,
            "INVALID_MATERIALIZATION_GENERATION",
        )

    expected_generation = (
        persisted_generation + 1
        if lifecycle_state == "RETRY_WAIT"
        else persisted_generation
    )
    return persisted_generation, expected_generation, None


def _diagnostic_base(
    row: Mapping[str, Any],
    meta: Mapping[str, Any],
    *,
    lifecycle_state: str,
    persisted_attempt: int | None,
    selector_attempt_number: int | None,
    attempt_source: str,
    persisted_generation: int | None,
) -> dict[str, Any]:
    cursor = meta.get("selector_recovery_cursor_v1")
    cursor_version = (
        cursor.get("version")
        if isinstance(cursor, Mapping)
        else None
    )

    return {
        "local_order_id": str(row.get("local_order_id") or "").strip(),
        "client_id": str(row.get("client_id") or "").strip().lower(),
        "execution_mode": str(
            row.get("execution_mode") or ""
        ).strip().lower(),
        "signal_id": str(row.get("signal_id") or "").strip(),
        "status": str(row.get("status") or "").strip().upper(),
        "lifecycle_state": lifecycle_state,
        "persisted_attempt": persisted_attempt,
        "selector_attempt_number": selector_attempt_number,
        "attempt_source": attempt_source,
        "materialization_generation": persisted_generation,
        "materialization_owner": str(
            meta.get("materialization_owner")
            or meta.get("retry_owner")
            or meta.get("current_owner")
            or ""
        ).strip(),
        "next_retry_at": (
            meta.get("materialization_next_retry_at")
            or meta.get("next_retry_at")
            or meta.get("deferred_retry_next_attempt_at")
        ),
        "updated_ts": row.get("updated_ts"),
        "cursor_present": isinstance(cursor, Mapping),
        "cursor_version": cursor_version,
    }


_MATERIALIZATION_STATUS_TO_LIFECYCLE: dict[str, str] = {
    "RETRY_PENDING": "RETRY_WAIT",
    "RUNNING": "MATERIALIZING",
}


def _canonical_lifecycle(
    meta: Mapping[str, Any],
) -> tuple[str, str | None]:
    """Resolve the effective startup-recovery lifecycle for a row.

    Mirrors the runtime compat rule: a row is in RETRY_WAIT when either
    ``lifecycle_state`` says so OR ``materialization_status`` is RETRY_PENDING;
    a row is MATERIALIZING when either field says so via RUNNING.

    When both fields are present, ``lifecycle_state`` wins (this matches OSM
    behavior — ``lifecycle_state`` is the field the OSM writes deterministically
    alongside every legal transition). A LIFECYCLE_STATUS_CONFLICT diagnostic
    is returned when the two fields disagree so an operator can see the hazard
    rather than having it silently downgraded.
    """
    lifecycle_raw = str(meta.get("lifecycle_state") or "").strip().upper()
    status_raw = str(
        meta.get("materialization_status") or ""
    ).strip().upper()
    status_lifecycle = _MATERIALIZATION_STATUS_TO_LIFECYCLE.get(status_raw, "")

    if lifecycle_raw in {"RETRY_WAIT", "MATERIALIZING"}:
        canonical = lifecycle_raw
        conflict = (
            bool(status_lifecycle)
            and status_lifecycle != canonical
        )
        return canonical, ("LIFECYCLE_STATUS_CONFLICT" if conflict else None)

    if status_lifecycle:
        return status_lifecycle, None

    return "", None


def classify_selector_recovery_hazard(
    raw_row: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return one deployment hazard, or None when the row is safe/inapplicable."""

    row = dict(raw_row or {})
    meta, meta_error = _coerce_meta(row.get("meta"))
    lifecycle_state, lifecycle_conflict = _canonical_lifecycle(meta)

    if lifecycle_state not in {"RETRY_WAIT", "MATERIALIZING"}:
        return None

    persisted_attempt, selector_attempt, attempt_source, attempt_error = (
        _attempt_state(lifecycle_state, meta)
    )
    persisted_generation, expected_generation, generation_error = (
        _generation_state(lifecycle_state, meta)
    )

    base = _diagnostic_base(
        row,
        meta,
        lifecycle_state=lifecycle_state,
        persisted_attempt=persisted_attempt,
        selector_attempt_number=selector_attempt,
        attempt_source=attempt_source,
        persisted_generation=persisted_generation,
    )

    for reason in (
        meta_error,
        lifecycle_conflict,
        attempt_error,
        generation_error,
    ):
        if reason:
            return {**base, "reason": reason}

    # Attempt one is allowed to initialize and persist its first cursor.
    if selector_attempt <= 1:
        return None

    local_order_id = base["local_order_id"]
    client_id = base["client_id"]
    execution_mode = base["execution_mode"]
    signal_id = base["signal_id"]

    if not local_order_id:
        return {**base, "reason": "MISSING_LOCAL_ORDER_ID"}
    if not client_id:
        return {**base, "reason": "MISSING_CLIENT_ID"}
    if execution_mode not in {"live", "paper"}:
        return {**base, "reason": "INVALID_EXECUTION_MODE"}
    if not signal_id:
        return {**base, "reason": "MISSING_SIGNAL_ID"}

    if (
        lifecycle_state == "MATERIALIZING"
        and not str(meta.get("materialization_owner") or "").strip()
    ):
        return {**base, "reason": "MISSING_MATERIALIZATION_OWNER"}

    cursor_candidate = meta.get("selector_recovery_cursor_v1")
    if cursor_candidate in (None, ""):
        return {**base, "reason": "MISSING_CURSOR_ON_RETRY"}

    # Runtime accepts a generation N-1 cursor only when the row's retry has
    # already been transactionally preclaimed by startup recovery (the code
    # path that sets sig["_recovery_pre_claimed"]=True and drives
    # allow_previous_generation=bool(_recovery_pre_claimed) in execution core).
    # That preclaim status is a runtime CAS artifact that cannot be proven
    # from a read-only SELECT of orders.meta. The preflight therefore requires
    # the EXACT expected generation to declare a row safe; a genuine
    # previous-generation cursor is treated as unsafe here and reported so
    # deployment stays fail-closed rather than silently agreeing with a state
    # only the running claimant could authorize.
    _, cursor_load_reason = load_selector_recovery_cursor(
        cursor_candidate,
        local_order_id=local_order_id,
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        materialization_generation=expected_generation,
        selector_attempt_count=selector_attempt,
        allow_previous_generation=False,
    )

    if cursor_load_reason:
        return {**base, "reason": str(cursor_load_reason)}

    return None


def classify_selector_recovery_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    hazards: list[dict[str, Any]] = []

    for raw_row in rows:
        hazard = classify_selector_recovery_hazard(raw_row)
        if hazard is not None:
            hazards.append(hazard)

    hazards.sort(
        key=lambda item: (
            str(item.get("execution_mode") or ""),
            str(item.get("client_id") or ""),
            str(item.get("updated_ts") or ""),
            str(item.get("local_order_id") or ""),
        )
    )
    return hazards


def _fetch_active_rows() -> list[dict[str, Any]]:
    # Import lazily so pure classification tests require no database.
    from ap.db import conn, run_with_retry

    def _read() -> list[dict[str, Any]]:
        with conn() as connection:
            connection.execute(ACTIVE_SELECTOR_RECOVERY_ROWS_SQL)
            return [
                dict(row)
                for row in (connection.fetchall() or [])
            ]

    return list(run_with_retry(_read) or [])


def run_preflight() -> list[dict[str, Any]]:
    return classify_selector_recovery_rows(_fetch_active_rows())


def main() -> int:
    try:
        hazards = run_preflight()
    except Exception as exc:
        payload = {
            "status": "ERROR",
            "unsafe_row_count": None,
            "rows": [],
            "error": f"{type(exc).__name__}:{exc}",
        }
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return 3

    payload = {
        "status": "FAIL" if hazards else "PASS",
        "unsafe_row_count": len(hazards),
        "rows": hazards,
    }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 2 if hazards else 0


if __name__ == "__main__":
    raise SystemExit(main())
