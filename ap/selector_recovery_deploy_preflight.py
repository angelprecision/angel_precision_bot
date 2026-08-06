"""Read-only deployment preflight for PR #401 selector recovery.

The command performs no writes and no broker calls. It uses the production
attempt resolver, cursor loader, cursor retry gate, canonical signal helper,
and retry-ceiling resolver. Exit 0 means candidate_row_count is zero (no
deferred-materialization candidates remain before deployment) and there is
no configuration conflict; exit 2 means candidate_row_count > 0 (even with
zero unsafe rows -- a row can be individually well-formed and still be an
in-flight candidate that must not be present at deploy time), or at least
one unsafe row, or a configuration conflict; exit 1 means the tool itself
failed.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from ap.db import conn
from ap.logger import get_logger
from ap_canonical_signal import build_canonical_signal_id

log = get_logger("ap.selector_recovery_deploy_preflight")

_EVIDENCE_KEYS = (
    "selector_recovery_cursor_v1",
    "materialization_lease_until",
    "materialization_owner",
    "materialization_generation",
    "current_owner",
    "watcher_token",
    "next_retry_at",
    "materialization_next_retry_at",
    "recovery_owner",
    "retry_attempt",
    "materialization_attempts",
    "breach_attempt_count",
)


def _fetch_candidate_rows() -> list[dict]:
    """Fetch every pending ENTRY row carrying any recovery evidence."""
    with conn() as c:
        rows = c.execute(
            """
            SELECT
                local_order_id, client_id, execution_mode, signal_id,
                canonical_signal_id, status, meta, updated_ts
            FROM orders
            WHERE UPPER(BTRIM(COALESCE(kind, ''))) = 'ENTRY'
              AND UPPER(BTRIM(COALESCE(status, ''))) = 'PENDING_TRIGGER'
              AND (
                    NULLIF(BTRIM(meta->>'lifecycle_state'), '') IS NOT NULL
                 OR NULLIF(BTRIM(meta->>'materialization_status'), '') IS NOT NULL
                 OR meta ? 'retry_attempt'
                 OR meta ? 'materialization_attempts'
                 OR meta ? 'breach_attempt_count'
                 OR meta ? 'selector_recovery_cursor_v1'
                 OR meta ? 'materialization_lease_until'
                 OR meta ? 'materialization_owner'
                 OR meta ? 'materialization_generation'
                 OR meta ? 'current_owner'
                 OR meta ? 'watcher_token'
                 OR meta ? 'next_retry_at'
                 OR meta ? 'materialization_next_retry_at'
                 OR meta ? 'recovery_owner'
                 OR meta ? 'trigger_crossed_at'
                 OR meta ? 'trigger_crossed_at_provenance'
                  )
            ORDER BY updated_ts ASC
            """
        ).fetchall()
    return rows


def _parse_timestamp(
    raw,
    *,
    finding: str,
    findings: list[str],
    require_timezone: bool = False,
) -> datetime | None:
    if raw in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            if require_timezone:
                findings.append(finding)
                return None
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        findings.append(finding)
        return None


def _classify_row(row: dict, *, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    meta = row.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (TypeError, ValueError):
            meta = {}
    if not isinstance(meta, dict):
        meta = {}

    findings: list[str] = []
    local_order_id = str(row.get("local_order_id") or "")
    lifecycle_raw = meta.get("lifecycle_state")
    materialization_status_raw = meta.get("materialization_status")
    lifecycle_state = str(lifecycle_raw) if lifecycle_raw is not None else ""
    materialization_status = (
        str(materialization_status_raw)
        if materialization_status_raw is not None
        else ""
    )

    # 1. Attempt counters: exact runtime resolver.
    from ap_execution_core import _resolve_selector_attempt_number

    resolved_attempt, conflict_reason = _resolve_selector_attempt_number(
        retry_attempt=meta.get("retry_attempt"),
        breach_attempt_count=meta.get("breach_attempt_count"),
        materialization_attempts=meta.get("materialization_attempts"),
        recovery_pre_claimed_attempt=None,
    )
    if conflict_reason:
        findings.append(f"COUNTER_CONFLICT:{conflict_reason}")

    # 2. Generation must exist for every evidence shape the query fetches.
    generation_raw = meta.get("materialization_generation")
    has_recovery_evidence = bool(
        lifecycle_state.strip()
        or materialization_status.strip()
        or any(key in meta for key in _EVIDENCE_KEYS)
    )
    generation_int: int | None = None
    if has_recovery_evidence:
        if generation_raw is None:
            findings.append("GENERATION_MISSING")
        elif isinstance(generation_raw, bool) or isinstance(generation_raw, float):
            findings.append("GENERATION_MALFORMED")
        elif isinstance(generation_raw, int):
            if generation_raw > 0:
                generation_int = generation_raw
            else:
                findings.append("GENERATION_MALFORMED")
        elif isinstance(generation_raw, str):
            try:
                parsed_generation = int(generation_raw.strip())
                if parsed_generation > 0:
                    generation_int = parsed_generation
                else:
                    findings.append("GENERATION_MALFORMED")
            except (TypeError, ValueError):
                findings.append("GENERATION_MALFORMED")
        else:
            findings.append("GENERATION_MALFORMED")

    # 3. Attempt 2+ requires a valid identity-bound durable cursor.
    if resolved_attempt is not None and resolved_attempt > 1:
        from ap.selector_retry_policy import load_selector_recovery_cursor
        from ap_execution_core import _selector_cursor_retry_block_reason

        cursor_candidate = meta.get("selector_recovery_cursor_v1")
        _cursor, cursor_load_reason = load_selector_recovery_cursor(
            cursor_candidate,
            local_order_id=local_order_id,
            client_id=str(row.get("client_id") or ""),
            execution_mode=str(row.get("execution_mode") or ""),
            signal_id=str(row.get("signal_id") or ""),
            materialization_generation=generation_int,
            selector_attempt_count=resolved_attempt,
            allow_previous_generation=False,
        )
        cursor_block_reason = _selector_cursor_retry_block_reason(
            cursor_enabled=True,
            selector_attempt_number=resolved_attempt,
            cursor_candidate=cursor_candidate,
            cursor_load_reason=cursor_load_reason,
        )
        if cursor_block_reason:
            findings.append(
                f"ATTEMPT_2PLUS_CURSOR_INVALID:{cursor_block_reason}"
            )

    # 4. Lifecycle pair allowlist and normalization.
    if lifecycle_state and lifecycle_state != lifecycle_state.strip():
        findings.append(f"LIFECYCLE_STATE_WHITESPACE_DRIFT:{lifecycle_state!r}")
    lifecycle_norm = lifecycle_state.strip().upper()
    if lifecycle_state.strip() and lifecycle_norm != lifecycle_state.strip():
        findings.append(f"LIFECYCLE_STATE_CASE_DRIFT:{lifecycle_state!r}")
    materialization_norm = materialization_status.strip().upper()
    valid_pairs = {
        ("MATERIALIZING", ""),
        ("MATERIALIZING", "RUNNING"),
        ("RETRY_WAIT", "RETRY_PENDING"),
        ("BROKER_READY", "SELECTED"),
        ("", "REARM_DIRECTION_REVERSAL"),
        ("", ""),
    }
    if (lifecycle_norm, materialization_norm) not in valid_pairs:
        findings.append(
            "LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT:"
            f"lifecycle={lifecycle_state}:status={materialization_status}"
        )

    # 5. Expired claims need a valid future handoff, not merely a timestamp key.
    lease_dt = _parse_timestamp(
        meta.get("materialization_lease_until"),
        finding="LEASE_TIMESTAMP_MALFORMED",
        findings=findings,
    )
    lease_expired = bool(lease_dt and lease_dt < now)
    next_retry_raw = meta.get("next_retry_at")
    materialization_next_raw = meta.get("materialization_next_retry_at")
    if (
        next_retry_raw not in (None, "")
        and materialization_next_raw not in (None, "")
        and str(next_retry_raw).strip() != str(materialization_next_raw).strip()
    ):
        findings.append("RETRY_SCHEDULE_CONFLICT")
    retry_schedule_raw = (
        next_retry_raw
        if next_retry_raw not in (None, "")
        else materialization_next_raw
    )
    retry_dt = _parse_timestamp(
        retry_schedule_raw,
        finding="RETRY_TIMESTAMP_MALFORMED",
        findings=findings,
    )
    has_retry_schedule = retry_schedule_raw not in (None, "")
    retry_schedule_stale = bool(retry_dt and retry_dt <= now)
    if lease_expired and not has_retry_schedule:
        findings.append("EXPIRED_LEASE_NO_RETRY_SCHEDULE")
    elif lease_expired and retry_dt is not None and retry_schedule_stale:
        findings.append("EXPIRED_LEASE_STALE_RETRY_SCHEDULE")

    # 6. Runtime identity: top-level fields must be complete and canonical.
    local_id_clean = local_order_id.strip()
    client_raw = str(row.get("client_id") or "")
    client_clean = client_raw.strip()
    signal_raw = str(row.get("signal_id") or "")
    signal_clean = signal_raw.strip()
    mode_raw = str(row.get("execution_mode") or "")
    mode_clean = mode_raw.strip().lower()
    canonical_raw = str(row.get("canonical_signal_id") or "")
    canonical_clean = canonical_raw.strip()

    for field, value in (
        ("local_order_id", local_id_clean),
        ("client_id", client_clean),
        ("signal_id", signal_clean),
        ("canonical_signal_id", canonical_clean),
    ):
        if not value:
            findings.append(f"MISSING_IDENTITY_FIELD:{field}")
    if mode_clean not in {"live", "paper"}:
        findings.append("INVALID_IDENTITY_FIELD:execution_mode")
    if local_order_id != local_id_clean and local_id_clean:
        findings.append("IDENTITY_NORMALIZATION_DRIFT:local_order_id")
    if client_raw != client_clean.lower() and client_clean:
        findings.append("IDENTITY_NORMALIZATION_DRIFT:client_id")
    if signal_raw != signal_clean and signal_clean:
        findings.append("IDENTITY_NORMALIZATION_DRIFT:signal_id")

    expected_canonical = build_canonical_signal_id(signal_clean)
    if canonical_clean and expected_canonical and canonical_clean != expected_canonical:
        findings.append("IDENTITY_MISMATCH:canonical_signal_id")

    mirror_rules = {
        "local_order_id": (local_id_clean, lambda value: str(value or "").strip()),
        "client_id": (client_clean.lower(), lambda value: str(value or "").strip().lower()),
        "signal_id": (signal_clean, lambda value: str(value or "").strip()),
        "execution_mode": (mode_clean, lambda value: str(value or "").strip().lower()),
        "canonical_signal_id": (
            canonical_clean,
            lambda value: str(value or "").strip(),
        ),
    }
    for field, (expected, normalize) in mirror_rules.items():
        if field in meta and normalize(meta.get(field)) != expected:
            findings.append(f"IDENTITY_MIRROR_MISMATCH:{field}")

    # 7. Trigger provenance must be complete, timestamp-valid, and bound to
    # this exact row identity. Presence alone is not proof: stale provenance
    # copied from another client/order/mode would otherwise pass the gate.
    timestamp_present = "trigger_crossed_at" in meta
    trigger_crossed_raw = meta.get("trigger_crossed_at")
    timestamp_blank = (
        isinstance(trigger_crossed_raw, str)
        and not trigger_crossed_raw.strip()
    )
    provenance_present = "trigger_crossed_at_provenance" in meta
    provenance = meta.get("trigger_crossed_at_provenance")

    if provenance_present and (
        not timestamp_present
        or trigger_crossed_raw is None
        or not str(trigger_crossed_raw or "").strip()
    ):
        findings.append("TRIGGER_CROSSED_TIMESTAMP_MISSING")

    if timestamp_present and trigger_crossed_raw is not None:
        if timestamp_blank:
            findings.append("TRIGGER_CROSSED_TIMESTAMP_MALFORMED")
        else:
            _parse_timestamp(
                trigger_crossed_raw,
                finding="TRIGGER_CROSSED_TIMESTAMP_MALFORMED",
                findings=findings,
                require_timezone=True,
            )
        if not provenance_present:
            findings.append("TRIGGER_PROVENANCE_MISSING")

    if provenance_present:
        if not isinstance(provenance, dict) or not all(
            provenance.get(key)
            for key in (
                "canonical_signal_id",
                "client_id",
                "execution_mode",
                "local_order_id",
            )
        ):
            findings.append("TRIGGER_PROVENANCE_INCOMPLETE")
        else:
            provenance_rules = {
                "local_order_id": (
                    local_id_clean,
                    lambda value: str(value or "").strip(),
                ),
                "client_id": (
                    client_clean.lower(),
                    lambda value: str(value or "").strip().lower(),
                ),
                "execution_mode": (
                    mode_clean,
                    lambda value: str(value or "").strip().lower(),
                ),
                "canonical_signal_id": (
                    canonical_clean,
                    lambda value: str(value or "").strip(),
                ),
            }
            for field, (expected, normalize) in provenance_rules.items():
                if normalize(provenance.get(field)) != expected:
                    findings.append(
                        f"TRIGGER_PROVENANCE_IDENTITY_MISMATCH:{field}"
                    )

    return {
        "local_order_id": local_order_id,
        "client_id": row.get("client_id"),
        "execution_mode": row.get("execution_mode"),
        "resolved_attempt": resolved_attempt,
        "generation": generation_raw,
        "lease_expired": lease_expired,
        "has_retry_schedule": has_retry_schedule,
        "retry_schedule_stale": retry_schedule_stale,
        "has_cursor": bool(meta.get("selector_recovery_cursor_v1")),
        "updated_ts": (
            row.get("updated_ts").isoformat()
            if hasattr(row.get("updated_ts"), "isoformat")
            else row.get("updated_ts")
        ),
        "safe": not findings,
        "findings": findings,
    }


def run_preflight() -> dict:
    from ap.selector_retry_policy import (
        DeferredMaterializationConfigConflict,
        resolve_deferred_materialization_max_attempts,
    )

    try:
        max_attempts = resolve_deferred_materialization_max_attempts()
        max_attempts_conflict = None
    except DeferredMaterializationConfigConflict as exc:
        max_attempts = None
        max_attempts_conflict = str(exc)

    rows = _fetch_candidate_rows()
    classified = [_classify_row(row) for row in rows]
    unsafe = [result for result in classified if not result["safe"]]
    return {
        "tool": "ap.selector_recovery_deploy_preflight",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "resolved_max_attempts": max_attempts,
        "max_attempts_config_conflict": max_attempts_conflict,
        "candidate_row_count": len(classified),
        "unsafe_row_count": len(unsafe),
        "rows": classified,
    }


def main() -> int:
    try:
        result = run_preflight()
    except Exception as exc:
        print(json.dumps({"tool_error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    # The documented release gate requires candidate_row_count == 0, not
    # merely unsafe_row_count == 0. "All discovered rows are internally
    # consistent" is materially weaker than "no deferred-materialization
    # candidates remain before deployment" -- a row can be individually
    # well-formed (safe=true) and still represent an in-flight
    # deferred-recovery candidate that must not be present at deploy time.
    return 2 if (
        result["candidate_row_count"] > 0
        or result["unsafe_row_count"] > 0
        or result.get("max_attempts_config_conflict") is not None
    ) else 0


if __name__ == "__main__":
    sys.exit(main())
