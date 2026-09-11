# P0 RECUT — Due deferred-materialization retry authority only

**Status:** DRAFT / HARD HOLD / CODEX IMPLEMENTATION REQUIRED  
**Base:** `main@80dbd631d78c489a36aa39b422c86829cb444b09`  
**Source investigation:** PR #596, especially original implementation commit `5be4546c0ef7339afa111d23138aa6fba17a7846`  
**Purpose:** extract only the production defect that caused #596 to exist. Do not carry forward #596's accumulated retry/recovery architecture.

## 1. Incident / exact failure class

Production-shaped MO/MMM rows had already crossed the entry trigger and the selector had already made a bounded retry decision. The durable row looked like:

```text
status = PENDING_TRIGGER
kind = ENTRY
watcher_audit.reason_code = trigger_ready
lifecycle_state = RETRY_WAIT
materialization_status = RETRY_PENDING
materialization_outcome = RETRY_LATER_DATA_UNAVAILABLE (or other existing retry outcome)
materialization_next_retry_at = valid aware timestamp
materialization_last_failure_at = valid aware timestamp
trigger_crossed_at = valid aware timestamp
absolute_entry_deadline = valid aware timestamp
materialization_generation >= 1
retry_attempt >= 1
materialization_attempts >= 1
breach_attempt_count >= 1
broker_ready = false
materialization_in_flight = false
broker_order_id = null
submitted_ts = null
```

The row was not asking recovery to infer a new trigger. It already had a canonical durable post-breach retry lifecycle.

Current `main` incorrectly allows the historical `watcher_audit=trigger_ready` marker to outrank that retry lifecycle:

```python
if watcher_reason == "trigger_ready":
    if is_active_materialization_in_flight(row):
        return PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
    return PendingTriggerClassification.STUCK_TRIGGER_READY
```

That means an exact `RETRY_WAIT / RETRY_PENDING` row is classified as `STUCK_TRIGGER_READY` instead of `WAITING_RETRYABLE`.

A second pre-existing guard then rejects legacy rows whose `trigger_crossed_at` exists but `trigger_crossed_at_provenance` was never atomically persisted. That guard is correct for ordinary trigger recovery, but too strong for the exact durable retry lifecycle above because the row's retry lineage is already the authority being resumed.

**The bug is authority precedence, not a missing retry executor.** Current main already contains the due-retry execution path and `resume_deferred_materialization_retry()` handoff. This recut must route the exact durable row to that existing path. It must not create another scheduler, materializer, retry loop, broker path, or lifecycle system.

## 2. Binding invariant

```text
confirmed trigger
+ exact durable RETRY_WAIT / RETRY_PENDING authority
+ valid retry lineage
+ no broker handoff
+ no active materializer
        ↓
WAITING_RETRYABLE
        ↓
existing due-retry executor
        ↓
existing resume_deferred_materialization_retry()
```

It must NOT become:

```text
trigger_ready
  ↓
STUCK_TRIGGER_READY / RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
  ↓
row remains stranded after retry time is due
```

## 3. Hard scope boundary

### Allowed production files — exactly these three

1. `ap/pending_trigger_classifier.py`
2. `ap_recovery.py`
3. `ap/pending_trigger_restart_recovery.py`

### Allowed test file

4. `tests/test_p0_pr596_due_materialization_retry_liveness.py`

### This spec file may remain in the PR

5. `docs/pr_specs/p0_pr596_due_retry_authority_recut_20260910.md`

### Forbidden production changes

Do **not** modify any of the following unless a fail-first test for the exact MO/MMM failure proves the three-file recut cannot work without it:

- `ap_execution_core.py`
- `ap/order_state_machine.py`
- `ap/order_monitor.py`
- `ap/deferred_materializer.py`
- `client_runner.py`
- `ap/preopen_readiness.py`
- `ap/pending_trigger_restart_recovery.py` beyond the narrow evidence-gate changes described below
- selector ranking / contract-selection behavior
- broker submit/cancel/replace behavior
- position mutation
- proof-trades mutation
- operator queue eligibility
- retry backoff formulas
- retry deadlines
- retry scheduler architecture
- readiness architecture
- runner supervision / restart architecture
- materialization claim semantics
- OSM CAS semantics

If implementation appears to require any forbidden surface, **stop and leave the PR DRAFT/HARD HOLD**. Do not broaden scope.

## 4. Production change A — recognize canonical durable retry authority

File: `ap/pending_trigger_classifier.py`

Add one read-only predicate adjacent to the existing pending-trigger/materialization predicates:

```python
def has_canonical_materialization_retry_authority(row: dict) -> bool:
    """Recognize the exact durable post-breach RETRY_PENDING handoff.

    This function is PURE and READ-ONLY.

    It does not claim the row, advance generation/attempt counters, request a
    quote, call the selector, submit/cancel a broker order, mutate a position,
    mutate proof_trades, or alter queue eligibility.

    Missing legacy trigger-crossed provenance is tolerated only when every
    other durable retry-authority field is complete and non-contradictory.
    If provenance is present, it must match exactly.
    """
    if not isinstance(row, dict):
        return False

    if str(row.get("status") or "").strip().upper() != "PENDING_TRIGGER":
        return False
    if str(row.get("kind") or "").strip().upper() != "ENTRY":
        return False

    local_order_id = str(row.get("local_order_id") or "").strip()
    client_id = str(row.get("client_id") or "").strip()
    execution_mode = str(row.get("execution_mode") or "").strip().lower()
    signal_id = str(row.get("signal_id") or "").strip()

    if not local_order_id or not client_id or not signal_id:
        return False
    if execution_mode not in {"live", "paper"}:
        return False

    if not _persisted_value_is_absent(row.get("broker_order_id")):
        return False
    if not _persisted_value_is_absent(row.get("submitted_ts")):
        return False

    meta = _coerce_classifier_meta(row.get("meta"))
    if not meta:
        return False

    watcher_audit = meta.get("watcher_audit")
    if not isinstance(watcher_audit, dict):
        return False
    if str(watcher_audit.get("reason_code") or "").strip().lower() != "trigger_ready":
        return False

    if str(meta.get("lifecycle_state") or "").strip().upper() != "RETRY_WAIT":
        return False
    if str(meta.get("materialization_status") or "").strip().upper() != "RETRY_PENDING":
        return False

    outcome = str(meta.get("materialization_outcome") or "").strip().upper()
    if outcome not in _RETRY_MATERIALIZATION_OUTCOMES:
        return False

    contract = str(row.get("contract") or "").strip().upper()
    if contract:
        if not contract.startswith("DEFERRED:"):
            return False
    elif meta.get("contract_deferred") is not True:
        return False

    if meta.get("broker_ready") is not False:
        return False
    if meta.get("materialization_in_flight") is not False:
        return False
    if has_broker_handoff_evidence(row):
        return False

    attempts = meta.get("materialization_attempts")
    retry_attempt = meta.get("retry_attempt")
    breach_attempt_count = meta.get("breach_attempt_count")
    generation = meta.get("materialization_generation")
    max_attempts = meta.get("retry_max_attempts")

    counters = (
        attempts,
        retry_attempt,
        breach_attempt_count,
        generation,
        max_attempts,
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in counters
    ):
        return False

    if attempts < 1:
        return False
    if retry_attempt != attempts:
        return False
    if breach_attempt_count != attempts:
        return False
    if generation < 1:
        return False
    if max_attempts < attempts:
        return False

    selector_failure = meta.get("materialization_selector_failure")
    if not isinstance(selector_failure, dict):
        return False
    retry_reason = str(selector_failure.get("reason_code") or "").strip()
    if not retry_reason:
        return False

    # Use the existing canonical retry taxonomy. Unknown/terminal reasons must
    # never gain retry authority through this helper.
    from ap.selector_retry_policy import is_retryable_selector_reason
    if not is_retryable_selector_reason(retry_reason):
        return False

    def _aware_timestamp(raw):
        parsed = _parse_iso_classifier(raw)
        if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)

    # The canonical schedule must exist. Do not decide due/not-due here; this
    # helper proves authority only. The existing recovery executor owns time.
    retry_at = _aware_timestamp(meta.get("materialization_next_retry_at"))
    if retry_at is None:
        return False

    if _aware_timestamp(meta.get("materialization_last_failure_at")) is None:
        return False
    if _aware_timestamp(meta.get("trigger_crossed_at")) is None:
        return False
    if _aware_timestamp(meta.get("absolute_entry_deadline")) is None:
        return False

    # Legacy exception: provenance may be absent on the stranded production
    # shape. If it is PRESENT, however, it is authority and must match exactly.
    if "trigger_crossed_at_provenance" in meta:
        provenance = meta.get("trigger_crossed_at_provenance")
        if not isinstance(provenance, dict):
            return False

        try:
            from ap_canonical_signal import build_canonical_signal_id
            expected_canonical_signal_id = str(
                row.get("canonical_signal_id")
                or meta.get("canonical_signal_id")
                or build_canonical_signal_id(signal_id)
            ).strip()
        except Exception:
            return False

        expected = {
            "canonical_signal_id": expected_canonical_signal_id,
            "client_id": client_id.lower(),
            "execution_mode": execution_mode,
            "local_order_id": local_order_id,
        }
        actual = {
            "canonical_signal_id": str(
                provenance.get("canonical_signal_id") or ""
            ).strip(),
            "client_id": str(
                provenance.get("client_id") or ""
            ).strip().lower(),
            "execution_mode": str(
                provenance.get("execution_mode") or ""
            ).strip().lower(),
            "local_order_id": str(
                provenance.get("local_order_id") or ""
            ).strip(),
        }
        if actual != expected:
            return False

    return True
```

### Important implementation note

The snippet above is the intended control contract, not permission to introduce another retry-policy subsystem. Reuse existing private helpers already in `ap/pending_trigger_classifier.py` (`_coerce_classifier_meta`, `_persisted_value_is_absent`, `_parse_iso_classifier`, `_RETRY_MATERIALIZATION_OUTCOMES`, `has_broker_handoff_evidence`). Keep imports minimal.

Do **not** make this predicate decide whether the retry is due. It proves the durable retry authority only. Existing recovery already owns due-time evaluation.

## 5. Production change B — retry authority outranks stale trigger_ready classification

Still in `ap/pending_trigger_classifier.py`, change only the existing `watcher_reason == "trigger_ready"` branch.

Current:

```python
if watcher_reason == "trigger_ready":
    if is_active_materialization_in_flight(row):
        return PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
    return PendingTriggerClassification.STUCK_TRIGGER_READY
```

Required:

```python
if watcher_reason == "trigger_ready":
    if is_active_materialization_in_flight(row):
        return PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
    if has_canonical_materialization_retry_authority(row):
        return PendingTriggerClassification.WAITING_RETRYABLE
    return PendingTriggerClassification.STUCK_TRIGGER_READY
```

Priority is binding:

1. active current materializer owner → `MATERIALIZATION_IN_FLIGHT`
2. exact durable retry authority → `WAITING_RETRYABLE`
3. otherwise historical `trigger_ready` with no live owner/retry authority → `STUCK_TRIGGER_READY`

Do not weaken `STUCK_TRIGGER_READY` for incomplete or contradictory rows.

## 6. Production change C — startup/runtime recovery legacy-provenance bridge

File: `ap_recovery.py`

Import the new predicate with the existing classifier helpers.

Locate `_recover_deferred_breach_lifecycles()` and the current confirmed-trigger evidence guard that constructs `_evidence_row` and calls:

```python
recovery_trigger_evidence_identity_is_proven(
    _evidence_row,
    local_order_id,
)
```

The exact canonical retry lifecycle must be the **only** exception to the missing legacy provenance hold.

Required pattern:

```python
_evidence_row = dict(order)
_evidence_row["meta"] = meta

_canonical_retry_after_trigger = (
    has_canonical_materialization_retry_authority(_evidence_row)
)

if (
    not recovery_trigger_evidence_identity_is_proven(
        _evidence_row,
        local_order_id,
    )
    and not _canonical_retry_after_trigger
):
    log.critical(
        "[%s] %s local_order_id=%s — preserving order unchanged",
        self.client_id,
        RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN,
        local_order_id,
    )
    result.setdefault("errors", []).append(
        f"{RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN}:{local_order_id}"
    )
    continue
```

### Binding safety rule

Do not replace or weaken `recovery_trigger_evidence_identity_is_proven()` globally. Ordinary recovery rows still require the existing proof. This exception exists only because the exact `RETRY_WAIT / RETRY_PENDING` lifecycle is itself the durable post-breach authority being resumed.

Do not alter the existing due-time decision or the existing call to `resume_deferred_materialization_retry()`.

## 7. Production change D — restart recovery must accept the same authority

File: `ap/pending_trigger_restart_recovery.py`

Import `has_canonical_materialization_retry_authority`.

Inside `recover_one_row()` after the existing:

```python
watcher_owned = self._check_watcher_owns(local_oid, row)
_evidence_proven = recovery_trigger_evidence_identity_is_proven(row, local_oid)
```

add:

```python
_canonical_retry_after_trigger = (
    has_canonical_materialization_retry_authority(row)
)
```

Change the first evidence fence from:

```python
if watcher_owned is not True and not _evidence_proven:
    return _reject_unproven_trigger_evidence()
```

to:

```python
if (
    watcher_owned is not True
    and not _evidence_proven
    and not _canonical_retry_after_trigger
):
    return _reject_unproven_trigger_evidence()
```

Change the later evidence fence from:

```python
if not _evidence_proven:
    return _reject_unproven_trigger_evidence()
```

to:

```python
if not _evidence_proven and not _canonical_retry_after_trigger:
    return _reject_unproven_trigger_evidence()
```

Finally, inside `_verify_materialization_retry_ownership()` there is a reread evidence check. Apply the same narrow exception to the **reread row**, not the stale outer row:

```python
_evidence_row = dict(order)
_evidence_row["meta"] = meta

_canonical_retry_after_trigger = (
    has_canonical_materialization_retry_authority(_evidence_row)
)

if (
    not recovery_trigger_evidence_identity_is_proven(
        _evidence_row,
        local_order_id,
    )
    and not _canonical_retry_after_trigger
):
    # preserve existing failure/log/return behavior
    ...
```

Do not add quote work, watcher registration, selector work, mutation, or broker calls to these evidence-gate branches.

## 8. Required fail-first tests

File: `tests/test_p0_pr596_due_materialization_retry_liveness.py`

Do not copy the full historical #596 test file. Rebuild a compact test file from current main.

### A. Exact MO/MMM classifier regression

Parametrize symbols `MO` and `MMM` with the production-shaped row described above.

Required before/after proof:

```python
assert classify_pending_trigger_row(
    row,
    watcher_owned=False,
) == PendingTriggerClassification.WAITING_RETRYABLE
```

This must fail on unmodified current `main` by returning `STUCK_TRIGGER_READY`.

### B. Future-due remains waiting, zero execution

Same exact row but `materialization_next_retry_at = now + 5 minutes`.

Run the real startup-recovery seam. Assert:

- `resume_deferred_materialization_retry()` not called
- selector not called
- broker submit/cancel not called
- status remains `PENDING_TRIGGER`
- lifecycle remains `RETRY_WAIT / RETRY_PENDING`

### C. Due retry executes exactly once through existing seam

Same exact row with `materialization_next_retry_at = now - 1 minute`.

Run the real startup-recovery seam. Assert:

- existing `resume_deferred_materialization_retry()` is called exactly once
- no new direct selector invocation is introduced by recovery
- no direct broker submit/cancel from recovery
- local_order_id/client_id/execution_mode/signal_id are the exact durable identities

Use a deterministic stub for the execution-core consumer if needed, but do not mock away the classifier/recovery branch under test.

### D. WFC positive control

Use a production-shaped WFC retry row and prove normal `WAITING_RETRYABLE` handling remains unchanged. This prevents a ticker-specific patch disguised as lifecycle logic.

### E. Present-but-conflicting provenance is HOLD

Start from valid MO row. Add:

```python
meta["trigger_crossed_at_provenance"] = {
    "canonical_signal_id": "wrong-signal",
    "client_id": correct_client,
    "execution_mode": "live",
    "local_order_id": correct_local_order_id,
}
```

Assert:

```python
assert not has_canonical_materialization_retry_authority(row)
assert classify_pending_trigger_row(...) != WAITING_RETRYABLE
```

No mutation, selector, broker, position, or proof call.

### F. Broker handoff contradiction is HOLD

Parametrize at least:

- `broker_order_id` present
- `submitted_ts` present
- `meta.broker_ready = True`
- `meta.submit_intent_at` present
- `meta.broker_submit_key` present

All must fail canonical retry authority and perform zero retry execution.

### G. Incomplete retry lineage is HOLD

Parametrize missing or inconsistent:

- `retry_attempt`
- `materialization_attempts`
- `breach_attempt_count`
- `materialization_generation`
- `retry_max_attempts`
- `materialization_next_retry_at`
- `materialization_last_failure_at`
- `trigger_crossed_at`
- `absolute_entry_deadline`
- `materialization_selector_failure.reason_code`

No field may be silently defaulted into authority.

### H. Terminal/unknown selector reason is not promoted

Use an existing terminal reason such as `OI_TOO_LOW` and one unknown reason. Assert canonical retry authority is false even if the rest of the row is shaped like `RETRY_PENDING`.

## 9. PostgreSQL production-shape proof

At least one regression must use the repository's existing disposable PostgreSQL test fixture pattern rather than an OSM fake.

Seed an orders row with:

```text
client_id = exact client
execution_mode = live
kind = ENTRY
status = PENDING_TRIGGER
contract = DEFERRED:MO (and one MMM case)
broker_order_id = NULL
submitted_ts = NULL
meta.lifecycle_state = RETRY_WAIT
meta.materialization_status = RETRY_PENDING
meta.materialization_outcome = RETRY_LATER_DATA_UNAVAILABLE
meta.watcher_audit.reason_code = trigger_ready
meta.trigger_crossed_at = valid aware timestamp
meta.trigger_crossed_at_provenance = ABSENT
meta.materialization_next_retry_at = due
```

Run the real recovery query + classification + due-routing seam and prove exactly one handoff to the existing retry consumer.

The PostgreSQL proof must assert there is no direct broker submit/cancel from recovery and no position/proof mutation.

## 10. Non-regression assertions

The PR must explicitly prove all of the following:

- ordinary `trigger_ready` rows with no complete retry authority still classify `STUCK_TRIGGER_READY`
- active materializer rows still classify `MATERIALIZATION_IN_FLIGHT`
- terminal materialization outcomes remain terminal
- invalidated watcher reasons remain terminal/unsafe under existing taxonomy
- pre-breach rows remain governed by existing provenance rules
- LIVE/PAPER identity remains exact; no cross-mode recovery
- client identity remains exact; no cross-client recovery
- broker handoff markers always block this retry-authority exception
- no new broker submit/cancel/replace authority
- no new position or proof-trades mutation
- no queue behavior change
- no selector ranking or filtering change
- no retry timing/backoff/deadline policy change

## 11. Diff-size / review gate

Before declaring implementation complete:

```bash
git diff --name-only main...HEAD
```

Expected production paths are only:

```text
ap/pending_trigger_classifier.py
ap_recovery.py
ap/pending_trigger_restart_recovery.py
```

Plus:

```text
tests/test_p0_pr596_due_materialization_retry_liveness.py
docs/pr_specs/p0_pr596_due_retry_authority_recut_20260910.md
```

If any other production file appears, keep **HARD HOLD** and explain why it was necessary. Do not silently expand scope.

## 12. CI / merge gates

Implementation is not merge-ready until all are true:

1. `git diff --check` passes.
2. Focused #596 recut tests pass.
3. Existing pending-trigger/restart-recovery/deferred-materialization P0 tests pass.
4. Full canonical P0 exact-head suite passes on one unchanged head SHA.
5. Separate merge-ref P0 job passes using the same canonical test inventory.
6. No rerolling production code between exact-head and merge-ref evidence.
7. PR remains DRAFT until an independent audit confirms the net diff is limited to this failure class.

## 13. Explicitly rejected historical #596 scope

The following later #596 work is not part of this recut merely because it appeared on the old branch:

- post-claim terminal CAS redesign
- new terminal fencing architecture
- retry timestamp alias redesign
- order-monitor retry hydration
- crash-after-selector recovery architecture
- watcher-vs-recovery race framework
- broader materialization generation/owner changes
- retry scheduler changes
- readiness changes
- runner supervision changes
- OSM claim changes

Those may represent real independent hardening work, but they are not required to repair the original MO/MMM due-retry liveness defect. If still needed, they require separate incident evidence and separately reviewable PRs.

## 14. Codex implementation instruction

Implement this spec **in this existing PR branch**. Do not open another PR. Do not merge or deploy.

Start by writing the fail-first MO/MMM classifier regression against the current base. Confirm it fails as `STUCK_TRIGGER_READY`. Then implement the read-only authority predicate and the three narrow evidence/classification bridges above. Run the focused tests, inspect the final diff, and reject any accidental changes outside the permitted files.

The goal is not to make deferred retries more sophisticated. The goal is one thing only:

> A complete, non-contradictory durable `RETRY_WAIT / RETRY_PENDING` row must be allowed to reach the retry executor that current main already has, even when its legacy `trigger_crossed_at_provenance` stamp is absent.

Anything beyond that is out of scope.