# P0 SPEC — Make post-cancel ENTRY retry bounded, anti-chase, and single-owner

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

Base SHA: `b43ce9c53433bd0479baa87e9757b50740adaa01`

This PR is a surgical Codex work order. It repairs the retry machinery that already exists. It does not authorize a new execution subsystem, relaxed entry gates, or aggressive chasing.

## One job

When an ENTRY was genuinely submitted but did not fill, and the broker-confirmed cancel reason represents an execution miss rather than a dead setup, permit **at most one** bounded retry only when the setup is still provably aligned.

The retry must be single-owner and must never run for a missed move, invalid thesis, wide spread, runaway quote, unknown reason, unproven alignment, or ambiguous broker state.

## Existing production path

Current main already has post-cancel retry machinery. Do not build another one.

### `ap/order_monitor.py`

Current path:

```text
stale/unfilled ENTRY
-> broker cancel request
-> broker terminal cancel confirmation
-> OSM CANCELED transition
-> _release_symbol_lock_for_canceled()
-> _maybe_arm_post_cancel_retry()
-> orders.meta.retry_status = ARMED
-> _check_armed_retries()
-> _submit_armed_retry()
-> ap.execution.process_signal(...)
```

Key current-main anchors on base SHA:

- `_check_stale_entry_cancel()` age-ceiling path: approximately **2850–3035**.
- `_maybe_arm_post_cancel_retry()` begins around **3090**.
- `_check_armed_retries()` begins around **3215**.
- `_submit_armed_retry()` begins around **3265**.
- `_handle_stale_entry()` and broker-confirmed retry hook are around **3440–3635**.

### `ap/post_cancel_retry.py`

Current engine already normalizes cancel reasons, checks alignment, builds the retry payload, and caps attempts.

Current-main defects found in that existing path:

1. `RETRYABLE_REASONS` currently includes `"missed_move"` even though a missed move is the explicit anti-chase terminal condition.
2. `evaluate_retry()` reads prior count from `meta["retry_attempts"]` **plural**, while `order_monitor` persists `meta["retry_attempt"]` **singular** and the retry payload also uses `retry_attempt`. The cap can therefore forget the previous attempt.
3. `_alignment_ok()` returns `None` when direction / entry reference / current spot cannot be proven, and `evaluate_retry()` currently treats `None` as allow. A money-moving retry must not arm on unproven alignment.
4. `_check_armed_retries()` SELECTs ARMED rows and submits them without first atomically claiming the ARMED lifecycle. Two monitor owners/restarts can race on the same durable retry intent.
5. `_submit_armed_retry()` currently calls `ap.execution.process_signal()`, which can call `broker.place_order()` directly. This PR must not silently expand or replace that routing architecture. Codex must prove the existing path preserves the required identity/risk contract before retaining it; if that proof fails within the hard file budget, STOP and report rather than inventing a third path.

## Required behavior

### A. MISSED_MOVE is terminal, never retryable

In `ap/post_cancel_retry.py`:

- remove `"missed_move"` from `RETRYABLE_REASONS`;
- add `"missed_move"` to `NON_RETRYABLE_REASONS`.

`_normalize_reason()` must continue mapping production text containing `MISSED_MOVE` to canonical `missed_move`.

Result:

```text
STALE_ENTRY_CANCEL MISSED_MOVE ...
-> canceled
-> evaluate_retry(...)
-> ABORT / NON_RETRYABLE_REASON
-> zero retry broker POST
```

Do not weaken the existing missed-move classifier merely to generate more fills.

### B. One canonical attempt counter

For this P0, **one retry maximum per original signal**.

Canonical durable field: `retry_attempt` singular.

Read compatibility:

```python
prior_retry = first_valid_int(
    meta.get("retry_attempt"),
    meta.get("retry_attempts"),       # legacy read only
    previous_retry_payload.get("retry_attempt"),
    default=0,
)
```

Write:

- write `retry_attempt` singular;
- preserve a legacy plural field only as diagnostic if required for backward compatibility, never as a second authority;
- effective maximum for this P0 is 1.

A second canceled retry order belonging to the same retry lineage must resolve to `MAX_ATTEMPTS_REACHED` and issue zero broker ENTRY POST.

Do not reset the count merely because a new local order ID was created for retry #1. Lineage must follow the original signal / retry root.

### C. Alignment must be proven

Change retry admission from:

```text
alignment False -> abort
alignment None -> allow
```

to:

```text
alignment True -> may continue
alignment False -> ALIGNMENT_LOST
alignment None -> ALIGNMENT_UNPROVEN
```

Required proof inputs:

- direction is exactly CALL or PUT;
- positive original signal/trigger entry reference;
- positive fresh underlying spot.

If any are missing/malformed, ABORT. Do not retry from stale metadata guesses.

### D. Broker-confirmed cancellation remains mandatory

Do not arm a retry merely because a cancel request was sent.

The existing `_handle_stale_entry()` boundary is correct:

- broker cancel requested;
- broker status must prove a terminal cancel status;
- OSM transition to CANCELED must succeed;
- only then may `_maybe_arm_post_cancel_retry()` run.

If broker truth is unknown/ambiguous, retain the existing reconcile/poll behavior. Zero retry submit.

### E. Atomically claim ARMED before any retry submit

`_check_armed_retries()` currently performs a SELECT and then submit. Replace the ownership boundary with a compare-and-swap claim using the existing `orders` row and existing DB connection helper.

A candidate is eligible only if all of these are still true at claim time:

```text
client_id == this monitor client
kind == ENTRY
status == CANCELED
meta.retry_status == ARMED
meta.retry_ready_at <= now
execution_mode is valid and matches this runtime
retry_attempt == 1
```

Claim transition:

```text
ARMED -> SUBMITTING
```

Persist at least:

```text
retry_status = SUBMITTING
retry_submit_claim_owner = <stable per-process/call token>
retry_submit_claimed_at = <UTC timestamp>
retry_attempt = 1
```

Requirements:

- use one conditional SQL UPDATE / CAS and require exactly one row returned/updated;
- do not SELECT then blindly overwrite full `meta`;
- preserve all existing JSONB metadata;
- only the claim winner may call `_submit_armed_retry()`;
- claim loser performs zero broker work;
- if claim DB write fails, perform zero broker work.

After canonical submit result:

- success -> existing `SUBMITTED` retry status and new local/broker order references;
- safe pre-broker reject -> `FAILED`/`ABORTED` with exact reason;
- ambiguous broker result after bytes may have left process -> **do not reset to ARMED**. Preserve reconcile-required truth instead of enabling a duplicate retry.

### F. Preserve exact retry lineage and execution identity

Retry payload/durable metadata must carry and preserve:

- original `client_id`;
- original `execution_mode` (`live|paper`);
- original `signal_id`;
- original `canonical_signal_id` when available;
- `retry_of_local_oid`;
- stable retry root / original local order ID;
- `retry_attempt=1`;
- ticker/symbol;
- direction;
- score/tier;
- trigger/entry reference;
- selected contract only as evidence, not permission to bypass fresh quote checks.

Any client or execution-mode mismatch -> ABORT before broker work.

Never allow a PAPER canceled order to create a LIVE retry or vice versa.

### G. Existing submit route must be proven, not assumed

Current `_submit_armed_retry()` calls:

```python
from ap.execution import process_signal
result = process_signal(self.broker, self.client_id, retry_payload)
```

`ap.execution.process_signal()` has its own direct broker submit path via `broker.place_order()`.

For this surgical PR, Codex may retain that call **only if focused tests prove** that the retry invocation still executes the production-required controls for this exact retry shape before broker POST, including:

- client activity/mode resolution;
- kill switch;
- position / capital authority;
- symbol/duplicate protection;
- fresh option quote;
- spread/chase protection;
- quantity/cost reservation;
- no pre-fill position/proof mutation;
- exactly one broker ENTRY POST maximum.

If proving/fixing those invariants requires editing `ap/execution.py`, `ap/queue.py`, `ap_master_control.py`, broker code, OSM, or another production file, **STOP and report the scope expansion. Do not edit those files in this PR.**

This prevents a liveness fix from becoming an accidental execution rewrite.

## HARD FILE BUDGET

### Production maximum

- `ap/post_cancel_retry.py`
- `ap/order_monitor.py`

No other production file is authorized.

If another production file appears necessary, STOP.

### Tests

Create:

- `tests/test_p0_post_cancel_retry_liveness_safety.py`

Run existing adjacent tests unchanged:

- `tests/test_phase5_post_cancel_retry.py`
- `tests/test_phase9_retry_wire_in.py`
- `tests/test_phase11_entry_fill_conversion.py`

Do not rewrite unrelated legacy tests to greenwash the patch.

## Suggested code changes

### `ap/post_cancel_retry.py`

Minimal shape:

```python
NON_RETRYABLE_REASONS = frozenset({
    ...,
    "missed_move",
})

RETRYABLE_REASONS = frozenset({
    "entry_max_age_normal_reached",
    "entry_max_age_aplus_reached",
    "stale_entry_timeout",
    "broker_transient_error",
    "broker_rejected_transient",
    "unfilled_at_ladder_top",
})

# one canonical lineage attempt
prior_retries = _resolve_prior_retry_attempt(meta, canceled_order)
next_attempt = prior_retries + 1
if next_attempt > 1:
    return ABORT(MAX_ATTEMPTS_REACHED)

align = _alignment_ok(...)
if align is None:
    return ABORT(ALIGNMENT_UNPROVEN)
if align is False:
    return ABORT(ALIGNMENT_LOST)
```

Do not add new retry reasons in this PR.

### `ap/order_monitor.py`

Add one small helper near the existing retry methods, e.g.:

```python
def _claim_armed_retry_for_submit(self, local_order_id: str, now_epoch: float) -> dict | None:
    """CAS ARMED -> SUBMITTING. Return claimed production row or None."""
```

It must patch JSONB rather than replacing full `meta`, and fence on client/status/kind/mode/attempt/current ARMED state.

Then `_check_armed_retries()` becomes conceptually:

```python
for candidate in due_rows:
    claimed = self._claim_armed_retry_for_submit(candidate["local_order_id"], now_epoch)
    if not claimed:
        continue
    self._submit_armed_retry(
        claimed["local_order_id"],
        claimed["contract"],
        claimed_meta["retry_payload"],
        claimed_meta,
    )
```

The due-row SELECT is discovery only. CAS is authority.

## Required tests

At minimum:

1. `MISSED_MOVE` -> ABORT; zero retry submit.
2. normal max-age unfilled + proven alignment -> ARM retry #1.
3. A+ max-age unfilled + proven alignment -> ARM retry #1.
4. unknown cancel reason -> fail closed.
5. spread/runaway/thesis invalid -> fail closed.
6. missing current underlying -> `ALIGNMENT_UNPROVEN`.
7. missing entry reference -> `ALIGNMENT_UNPROVEN`.
8. invalid direction -> `ALIGNMENT_UNPROVEN` or existing specific missing-direction block; zero submit.
9. singular `retry_attempt=1` -> second retry rejected.
10. legacy plural `retry_attempts=1` -> second retry rejected.
11. previous retry payload `retry_attempt=1` -> second retry rejected even if outer meta lost the field.
12. ARMED CAS winner submits once.
13. two concurrent claim attempts -> one winner, one loser, one submit maximum.
14. stale candidate selected before peer changes status -> CAS loses, zero submit.
15. DB claim failure -> zero submit.
16. wrong client -> zero submit.
17. PAPER/LIVE mismatch -> zero submit.
18. malformed execution mode -> zero submit.
19. broker cancel requested but not terminal-confirmed -> retry never armed.
20. OSM CANCELED transition fails -> retry never armed.
21. retry payload preserves signal/canonical identity and retry root.
22. process_signal pre-broker rejection -> exact retry failure reason persisted, no second arm.
23. successful retry -> old canceled row references new local order and broker order; no position/proof mutation is fabricated by retry orchestrator.
24. crash/race after SUBMITTING claim cannot return row to ARMED automatically.
25. exact broker-call counting proves at most one retry ENTRY POST.

## Mandatory validation

```bash
python -m pytest -q \
  tests/test_p0_post_cancel_retry_liveness_safety.py \
  tests/test_phase5_post_cancel_retry.py \
  tests/test_phase9_retry_wire_in.py \
  tests/test_phase11_entry_fill_conversion.py
python -m py_compile \
  ap/post_cancel_retry.py \
  ap/order_monitor.py \
  tests/test_p0_post_cancel_retry_liveness_safety.py
git diff --check
```

## Frozen non-goals

Do not change:

- scanner or signal generation;
- score floors;
- trigger/watch logic;
- selector delta/DTE/moneyness/spread/OI/volume gates;
- position sizing percentages;
- Master Control cap formulas;
- broker adapter;
- cancellation semantics before broker confirmation;
- exit engine;
- position manager;
- proof trades;
- deferred materialization;
- overnight reevaluation;
- database schema/migrations;
- environment configuration;
- general `process_signal()` behavior;
- queue architecture;
- retry count above one;
- PAPER fill policy;
- LIVE execution pricing policy.

## Money-path safety checklist

- **Live behavior?** Yes, retry liveness can create one additional ENTRY attempt after a confirmed no-fill cancel.
- **Flag-off?** Existing `ENTRY_RETRY_ENABLED` remains the emergency kill switch. Do not add another flag unless absolutely necessary.
- **Broker submit/cancel?** Uses existing cancel path; retry can reach existing ENTRY submit path only after CAS ownership and all required gates. No new cancel path.
- **Orders mutation?** Existing canceled order meta is patched for retry ownership/status. New retry order may be created only through the existing approved entry path.
- **Positions/proof_trades?** Zero mutation by retry orchestrator before broker fill truth.
- **Queue?** No new queue writer in this PR.
- **client_id/execution_mode?** Exact preservation required and checked before submit.
- **Production metadata shape?** Tests must use the existing JSONB `orders.meta` fields, including both legacy plural and canonical singular attempt shapes.
- **Diagnostics?** Preserve exact cancel reason, normalized reason, alignment result, attempt, claim owner/time, submit result, and new order IDs.
- **Paper/live taxonomy?** Separate and exact; no cross-mode adoption.
- **Could this make Jason trade junk?** Not if this spec is followed: MISSED_MOVE becomes terminal, unknown/unproven alignment becomes terminal, and no quality/risk gate is loosened.

## Codex implementation instruction

Implement only this work order on this branch.

First read the current implementations of `evaluate_retry`, `_maybe_arm_post_cancel_retry`, `_check_armed_retries`, `_submit_armed_retry`, and `_handle_stale_entry`. Reuse them. Do not create parallel methods unless the single CAS helper described above is needed.

Before retaining `process_signal()` as the retry submit seam, prove from current code/tests that it satisfies the required pre-broker controls for this retry shape. If that cannot be proved without touching a third production file, STOP and report the exact missing invariant.

After implementation report:

1. exact changed production lines;
2. why every retryable reason is safe;
3. exact attempt-count authority;
4. concurrent claim proof;
5. broker ENTRY POST count proof;
6. `client_id` / `execution_mode` preservation;
7. orders/positions/proof/queue mutation exposure;
8. focused test results;
9. complete changed-file list.

**No merge, deploy, migration, environment mutation, or LIVE authority change is authorized by this spec.**