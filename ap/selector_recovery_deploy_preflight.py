"""ap/selector_recovery_deploy_preflight.py — read-only deployment preflight
for PR #401's selector-recovery / deferred-materialization retry system.

Run via:
    python -m ap.selector_recovery_deploy_preflight

It performs no writes, no broker calls, no lifecycle transitions, no
submissions, no cancellations, no cursor adoption. It reads every
PENDING_TRIGGER/MATERIALIZING ENTRY row and classifies it using the SAME
production resolver helpers runtime code uses -- not reimplemented logic --
so preflight classification can never drift from actual runtime behavior:

  - ap_execution_core._resolve_selector_attempt_number  (Blocker 1 fix)
  - ap.selector_retry_policy.resolve_deferred_materialization_max_attempts
    (Blocker 2 fix)
  - ap.selector_retry_policy.load_selector_recovery_cursor

Output is deterministic JSON to stdout. Exit code is 0 if every row
classifies as safe, 2 if any row is unsafe (counter conflict, malformed/
missing attempt-2+ cursor, lifecycle/materialization conflict, expired
lease with no retry schedule, incomplete identity, or incomplete trigger
provenance), 1 on a genuine tool error (DB unreachable, etc.).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from ap.db import conn
from ap.logger import get_logger

log = get_logger("ap.selector_recovery_deploy_preflight")


def _fetch_candidate_rows() -> list[dict]:
    with conn() as c:
        rows = c.execute(
            """
            SELECT
                local_order_id, client_id, execution_mode, signal_id,
                canonical_signal_id, status, meta, updated_ts
            FROM orders
            WHERE UPPER(COALESCE(kind, '')) = 'ENTRY'
              AND UPPER(COALESCE(status, '')) = 'PENDING_TRIGGER'
              AND meta->>'lifecycle_state' = 'MATERIALIZING'
            ORDER BY updated_ts ASC
            """
        ).fetchall()
    return rows


def _classify_row(row: dict, *, now: datetime | None = None) -> dict:
    """Classify a single candidate row. Returns a dict with at minimum
    "local_order_id", "safe" (bool), and "findings" (list[str])."""
    now = now or datetime.now(timezone.utc)
    meta = row.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (TypeError, ValueError):
            meta = {}

    findings: list[str] = []
    local_order_id = str(row.get("local_order_id") or "")

    # 1. Counter conflict -- reuse the exact runtime resolver.
    from ap_execution_core import _resolve_selector_attempt_number

    resolved_attempt, conflict_reason = _resolve_selector_attempt_number(
        retry_attempt=meta.get("retry_attempt"),
        breach_attempt_count=meta.get("breach_attempt_count"),
        materialization_attempts=meta.get("materialization_attempts"),
        recovery_pre_claimed_attempt=None,
    )
    if conflict_reason:
        findings.append(f"COUNTER_CONFLICT:{conflict_reason}")

    # 2. Missing/malformed attempt-2+ cursor -- reuse the exact runtime
    # cursor loader.
    if resolved_attempt is not None and resolved_attempt > 1:
        from ap.selector_retry_policy import load_selector_recovery_cursor

        _generation = meta.get("materialization_generation")
        try:
            _generation_int = int(_generation) if _generation is not None else None
        except (TypeError, ValueError):
            _generation_int = None
            findings.append("GENERATION_MALFORMED")

        _cursor, cursor_reason = load_selector_recovery_cursor(
            meta.get("selector_recovery_cursor_v1"),
            local_order_id=local_order_id,
            client_id=str(row.get("client_id") or ""),
            execution_mode=str(row.get("execution_mode") or ""),
            signal_id=str(row.get("signal_id") or ""),
            materialization_generation=_generation_int,
            selector_attempt_count=resolved_attempt,
            allow_previous_generation=False,
        )
        if cursor_reason:
            findings.append(f"ATTEMPT_2PLUS_CURSOR_INVALID:{cursor_reason}")

    # 3. Lifecycle/materialization-status conflict.
    lifecycle_state = str(meta.get("lifecycle_state") or "")
    materialization_status = str(meta.get("materialization_status") or "")
    if lifecycle_state == "MATERIALIZING" and materialization_status in (
        "SELECTED", "REARM_DIRECTION_REVERSAL", "FAILED_TERMINAL",
    ):
        findings.append(
            f"LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT:"
            f"lifecycle={lifecycle_state}:status={materialization_status}"
        )

    # 4. Expired MATERIALIZING lease with no retry schedule.
    lease_raw = meta.get("materialization_lease_until")
    lease_expired = False
    if lease_raw:
        try:
            lease_dt = datetime.fromisoformat(str(lease_raw).replace("Z", "+00:00"))
            if lease_dt.tzinfo is None:
                lease_dt = lease_dt.replace(tzinfo=timezone.utc)
            lease_expired = lease_dt < now
        except (TypeError, ValueError):
            findings.append("LEASE_TIMESTAMP_MALFORMED")
    has_retry_schedule = bool(meta.get("next_retry_at"))
    if lease_expired and not has_retry_schedule:
        findings.append("EXPIRED_LEASE_NO_RETRY_SCHEDULE")

    # 5. Incomplete identity.
    for field in ("client_id",):
        if not str(row.get(field) or "").strip():
            findings.append(f"MISSING_IDENTITY_FIELD:{field}")
    if not str(row.get("execution_mode") or "").strip():
        findings.append("MISSING_IDENTITY_FIELD:execution_mode")

    # 6. Incomplete trigger provenance (only relevant if a breach was
    # actually confirmed).
    if meta.get("trigger_crossed_at") and not meta.get(
        "trigger_crossed_at_provenance"
    ):
        findings.append("TRIGGER_PROVENANCE_MISSING")
    elif meta.get("trigger_crossed_at_provenance"):
        prov = meta["trigger_crossed_at_provenance"]
        if not isinstance(prov, dict) or not all(
            prov.get(k) for k in (
                "canonical_signal_id", "client_id", "execution_mode",
                "local_order_id",
            )
        ):
            findings.append("TRIGGER_PROVENANCE_INCOMPLETE")

    return {
        "local_order_id": local_order_id,
        "client_id": row.get("client_id"),
        "execution_mode": row.get("execution_mode"),
        "resolved_attempt": resolved_attempt,
        "generation": meta.get("materialization_generation"),
        "lease_expired": lease_expired,
        "has_cursor": bool(meta.get("selector_recovery_cursor_v1")),
        "updated_ts": (
            row.get("updated_ts").isoformat()
            if hasattr(row.get("updated_ts"), "isoformat")
            else row.get("updated_ts")
        ),
        "safe": len(findings) == 0,
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
        max_attempts = 3
        max_attempts_conflict = str(exc)

    rows = _fetch_candidate_rows()
    classified = [_classify_row(row) for row in rows]
    unsafe = [r for r in classified if not r["safe"]]

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
    except Exception as exc:  # genuine tool error, not a row classification
        print(json.dumps({"tool_error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 2 if result["unsafe_row_count"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
