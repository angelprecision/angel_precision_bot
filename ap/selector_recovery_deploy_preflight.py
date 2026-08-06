"""ap/selector_recovery_deploy_preflight.py — read-only deployment preflight
for PR #401's selector-recovery / deferred-materialization retry system.

Run via:
    python -m ap.selector_recovery_deploy_preflight

It performs no writes, no broker calls, no lifecycle transitions, no
submissions, no cancellations, no cursor adoption. It reads every
PENDING_TRIGGER/MATERIALIZING ENTRY row and classifies it using the SAME
production resolver helpers runtime code uses -- not reimplemented logic --
so preflight classification can never drift from actual runtime behavior:

  - ap_execution_core._resolve_selector_attempt_number  (counter conflict)
  - ap_execution_core._selector_cursor_retry_block_reason  (Blocker 1 fix:
    MISSING_CURSOR_ON_RETRY detected even when load_selector_recovery_cursor
    silently produces a fresh cursor for a missing candidate)
  - ap.selector_retry_policy.resolve_deferred_materialization_max_attempts
    (Blocker 2 fix: config conflict forces exit 2 even with zero rows)
  - ap.selector_retry_policy.load_selector_recovery_cursor

Output is deterministic JSON to stdout. Exit code is 0 if every row
classifies as safe AND there is no configuration conflict, 2 if any row is
unsafe (counter conflict, malformed/missing attempt-2+ cursor, lifecycle/
materialization conflict, expired lease with no retry schedule, incomplete
identity, incomplete trigger provenance, or invalid/missing generation) OR if
a configuration conflict is detected, 1 on a genuine tool error (DB
unreachable, etc.).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from ap.db import conn
from ap.logger import get_logger

log = get_logger("ap.selector_recovery_deploy_preflight")


def _fetch_candidate_rows() -> list[dict]:
    """Fetch every PENDING_TRIGGER ENTRY row carrying ANY deferred-
    materialization evidence -- not just the one exact
    lifecycle_state='MATERIALIZING' combination. A classifier that only
    queries the state it already expects cannot find the inconsistent
    states it exists to detect (case/whitespace drift on lifecycle_state,
    blank lifecycle with an active materialization_status, RUNNING or
    retry-owned states outside the one exact combination, a cursor or
    lease present with no matching lifecycle marker, etc.). This casts a
    deliberately wide net on any evidence of deferred-materialization
    involvement; _classify_row() does the actual safety determination for
    every row this returns.

    Blocker 4 fix: BTRIM applied to both kind and status outer predicates
    so rows with leading/trailing whitespace (e.g. kind=' ENTRY ',
    status=' PENDING_TRIGGER ') are not silently invisible.
    """
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
                    -- Any lifecycle_state at all (case/whitespace drift,
                    -- or a value other than exactly "MATERIALIZING").
                    NULLIF(BTRIM(meta->>'lifecycle_state'), '') IS NOT NULL
                    -- Blank/absent lifecycle but an active materialization
                    -- status still present.
                 OR NULLIF(BTRIM(meta->>'materialization_status'), '') IS NOT NULL
                    -- Any attempt counter present.
                 OR meta ? 'retry_attempt'
                 OR meta ? 'materialization_attempts'
                 OR meta ? 'breach_attempt_count'
                    -- A durable cursor present.
                 OR meta ? 'selector_recovery_cursor_v1'
                    -- Lease/lock/ownership/generation metadata present.
                 OR meta ? 'materialization_lease_until'
                 OR meta ? 'materialization_owner'
                 OR meta ? 'materialization_generation'
                 OR meta ? 'current_owner'
                 OR meta ? 'watcher_token'
                    -- A retry schedule present.
                 OR meta ? 'next_retry_at'
                    -- Recovery-owner metadata (restart-recovery path).
                 OR meta ? 'recovery_owner'
                  )
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

    # Extract lifecycle/status up-front -- reused across multiple checks
    # below so the extraction is not repeated.
    lifecycle_raw = meta.get("lifecycle_state")
    materialization_status_raw = meta.get("materialization_status")
    lifecycle_state = str(lifecycle_raw) if lifecycle_raw is not None else ""
    materialization_status = (
        str(materialization_status_raw) if materialization_status_raw is not None else ""
    )

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

    # 2. Generation completeness -- validated independently of attempt
    # number (Blocker 3 fix).  Generation is part of the ownership and
    # cursor identity fence; any row carrying active deferred-
    # materialization lifecycle or ownership evidence must carry a valid
    # positive integer generation.  Missing, zero, negative, boolean,
    # float, or malformed generation is unsafe regardless of what attempt
    # number the row reports.
    # Evidence predicate mirrors the broadened candidate query's own
    # deferred-materialization evidence set exactly -- any field that can
    # cause a row to be *fetched* as deferred-materialization evidence must
    # also cause generation to be *required*.  Key presence (not truthiness)
    # is used deliberately: an explicitly empty ownership field (e.g.
    # current_owner="") is itself malformed evidence, not the same as the
    # field being absent, and must not quietly exempt the row from the
    # generation requirement.
    _generation_raw = meta.get("materialization_generation")
    _has_ownership_evidence = bool(
        lifecycle_state.strip()
        or materialization_status.strip()
        or "selector_recovery_cursor_v1" in meta
        or "materialization_lease_until" in meta
        or "materialization_owner" in meta
        or "current_owner" in meta
        or "watcher_token" in meta
        or "recovery_owner" in meta
        or "next_retry_at" in meta
        or "retry_attempt" in meta
        or "materialization_attempts" in meta
        or "breach_attempt_count" in meta
    )
    _generation_int: int | None = None
    if _has_ownership_evidence:
        if _generation_raw is None:
            findings.append("GENERATION_MISSING")
        elif isinstance(_generation_raw, bool):
            # bool is a subclass of int in Python; must be checked before int.
            findings.append("GENERATION_MALFORMED")
        elif isinstance(_generation_raw, float):
            findings.append("GENERATION_MALFORMED")
        elif isinstance(_generation_raw, int):
            if _generation_raw <= 0:
                findings.append("GENERATION_MALFORMED")
            else:
                _generation_int = _generation_raw
        elif isinstance(_generation_raw, str):
            # Accept integer-shaped strings (e.g. "1"); reject fractional,
            # non-numeric, or zero/negative.
            try:
                _val = int(_generation_raw.strip())
                if _val > 0:
                    _generation_int = _val
                else:
                    findings.append("GENERATION_MALFORMED")
            except (TypeError, ValueError):
                findings.append("GENERATION_MALFORMED")
        else:
            findings.append("GENERATION_MALFORMED")

    # 3. Missing/malformed attempt-2+ cursor -- reuse both the exact
    # runtime cursor loader AND the runtime cursor retry-block helper
    # (Blocker 1 fix).
    #
    # The loader alone is insufficient: load_selector_recovery_cursor()
    # returns a fresh cursor with no cursor_reason when the candidate is
    # absent (None/""), so an attempt-2+ row with no durable cursor
    # previously classified as safe.  _selector_cursor_retry_block_reason()
    # is the production gate that explicitly returns MISSING_CURSOR_ON_RETRY
    # when attempt > 1 and cursor_candidate is None/"".  The preflight now
    # calls both helpers in the same order runtime does.
    if resolved_attempt is not None and resolved_attempt > 1:
        from ap.selector_retry_policy import load_selector_recovery_cursor
        from ap_execution_core import _selector_cursor_retry_block_reason

        _cursor_candidate = meta.get("selector_recovery_cursor_v1")
        _cursor, cursor_load_reason = load_selector_recovery_cursor(
            _cursor_candidate,
            local_order_id=local_order_id,
            client_id=str(row.get("client_id") or ""),
            execution_mode=str(row.get("execution_mode") or ""),
            signal_id=str(row.get("signal_id") or ""),
            materialization_generation=_generation_int,
            selector_attempt_count=resolved_attempt,
            allow_previous_generation=False,
        )
        _cursor_block_reason = _selector_cursor_retry_block_reason(
            cursor_enabled=True,
            selector_attempt_number=resolved_attempt,
            cursor_candidate=_cursor_candidate,
            cursor_load_reason=cursor_load_reason,
        )
        if _cursor_block_reason:
            findings.append(f"ATTEMPT_2PLUS_CURSOR_INVALID:{_cursor_block_reason}")

    # 4. Lifecycle/materialization-status conflict -- allowlist of known-
    # valid pairings rather than a denylist of known-bad ones, so any
    # combination this classifier doesn't already recognize is flagged by
    # default rather than silently passing through.
    if lifecycle_state and lifecycle_state != lifecycle_state.strip():
        findings.append(f"LIFECYCLE_STATE_WHITESPACE_DRIFT:{lifecycle_state!r}")
    _lifecycle_norm = lifecycle_state.strip().upper()
    if lifecycle_state.strip() and _lifecycle_norm != lifecycle_state.strip():
        findings.append(f"LIFECYCLE_STATE_CASE_DRIFT:{lifecycle_state!r}")
    _materialization_norm = materialization_status.strip().upper()

    _known_valid_pairs = {
        ("MATERIALIZING", ""),
        ("MATERIALIZING", "RUNNING"),
        ("RETRY_WAIT", "RETRY_PENDING"),
        ("BROKER_READY", "SELECTED"),
        ("", "REARM_DIRECTION_REVERSAL"),
        ("", ""),
    }
    if (_lifecycle_norm, _materialization_norm) not in _known_valid_pairs:
        findings.append(
            f"LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT:"
            f"lifecycle={lifecycle_state}:status={materialization_status}"
        )

    # 5. Expired MATERIALIZING lease with no retry schedule.
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

    # 6. Incomplete identity.
    for field in ("client_id",):
        if not str(row.get(field) or "").strip():
            findings.append(f"MISSING_IDENTITY_FIELD:{field}")
    if not str(row.get("execution_mode") or "").strip():
        findings.append("MISSING_IDENTITY_FIELD:execution_mode")

    # 7. Incomplete trigger provenance (only relevant if a breach was
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
    # Blocker 2 fix: do not substitute a numeric fallback.  A configuration
    # conflict is an explicitly unsafe condition -- the deployment gate must
    # not bless a runtime whose canonical retry authority is contradictory.
    # resolved_max_attempts=null signals "unknown due to conflict"; the
    # presence of max_attempts_config_conflict is what drives exit 2 in
    # main(), independently of unsafe_row_count.
    try:
        max_attempts = resolve_deferred_materialization_max_attempts()
        max_attempts_conflict = None
    except DeferredMaterializationConfigConflict as exc:
        max_attempts = None          # no local fallback -- ever
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
    # Blocker 2 fix: exit 2 on any unsafe row OR any configuration conflict,
    # even when candidate_row_count == 0.  A deployment gate that reports
    # success while the canonical retry authority is explicitly contradictory
    # is not a deployment gate.
    _has_unsafe_rows = result["unsafe_row_count"] > 0
    _has_config_conflict = result.get("max_attempts_config_conflict") is not None
    return 2 if (_has_unsafe_rows or _has_config_conflict) else 0


if __name__ == "__main__":
    sys.exit(main())
