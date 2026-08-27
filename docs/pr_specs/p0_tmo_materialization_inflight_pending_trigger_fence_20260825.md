# P0 — TMO materialization-in-flight fence for PENDING_TRIGGER recovery

## AMENDMENT — BINDING (2026-08-27, supersedes broadening in this spec)

This spec is retained for historical context, but the following amendment is
the binding contract for PR #521. Where the spec below appears to authorize
broader lifecycle protection, this amendment narrows it.

**Rebased on post-#524 main:**
`ba1e86a01ed9c81423cc6b5baaa766e43310bec7`

### Amendment round 2 (2026-08-27, follow-up audit) — fail-closed strictness

Two fail-closed gaps closed:

1. **`_active_materialization_proof`: `materialization_generation` must be a
   real `int`.** Pre-r2 code did `int(generation)` inside a try/except, which
   silently accepted the string `"1"`. #524 writes this field as a PostgreSQL
   integer; anything else is a schema anomaly. The predicate now requires
   `isinstance(int) and not isinstance(bool)` with no coercion — string,
   float, bool, and None all fail closed at their exact durable shape.

2. **`order_monitor._maybe_hydrate_deferred_order`: durable
   `execution_mode` must not be inferred from runner context.** Pre-r2
   code did `str(order.get("execution_mode") or self.client_mode or "")`,
   so a row with `execution_mode = NULL / "" / whitespace / "banana"`
   would inherit the runner's mode and could then receive the
   materialization-in-flight fence. This was inconsistent with the
   classifier and `ap_recovery`, both of which reject missing/malformed
   durable mode before classification. The hydration guard now fails
   closed with `reason=execution_mode_missing_or_invalid` before the
   materialization guard is consulted.

Production check preceding r2: `SELECT COUNT(*)` over active deferred
`PENDING_TRIGGER ENTRY` rows on 2026-08-27 = 36; blank `execution_mode` = 0;
string `materialization_generation` = 0. No currently poisoned rows —
r2 closes the fail-closed contract, not a live incident.

Negative controls added:

- `TestActiveMaterializationProof::test_generation_string_rejected` —
  `"1"`, `"01"`, `" 1 "`, `"0"`, `"-1"` all → `False`.
- `TestActiveMaterializationProof::test_generation_float_rejected` —
  `1.0`, `2.5`, `0.0`, `-1.0` all → `False`.
- `test_deferred_prebreach_hydration.py::test_hydration_refuses_to_infer_missing_or_malformed_execution_mode` —
  parametrized over `None`, `""`, `"   "`, `"banana"`, `"LIVE_OR_PAPER"`;
  each → `reason=execution_mode_missing_or_invalid`, guard NOT invoked.
- `test_deferred_prebreach_hydration.py::test_hydration_execution_mode_mismatch_still_wins_over_materialization_guard` —
  PAPER durable row on LIVE runner → `reason=execution_mode_mismatch`,
  guard NOT invoked, even with full active-owner metadata.

### Binding scope after amendment (r1 + r2):

- Core canonical work in `ap/pending_trigger_classifier.py` and
  `ap/pending_trigger_restart_recovery.py` remains as authored:
  `MATERIALIZATION_IN_FLIGHT` classification and `MATERIALIZATION_OWNED`
  recovery outcome, gated on FULL canonical proof
  (`is_active_materialization_in_flight`).
- Every retained consumer edit MUST route through the single shared
  canonical predicate `is_active_materialization_in_flight`. No partial
  markers (`RUNNING` alone, `QUEUED` alone, `MATERIALIZING` alone,
  `materialization_in_flight=true` alone, `broker_ready` alone,
  `broker_submit_*` alone) may confer active-materializer protection.
- Partial or malformed materialization metadata MUST remain cleanable by
  pre-existing cleanup/recovery authority. A crashed row leaving one stale
  `RUNNING`/`QUEUED` marker MUST NOT become immortal.

**Retained consumer edits (each closes a proven bypass with the canonical
predicate):**

1. `ap/order_monitor.py`:
   - `MATERIALIZATION_OWNED` outcome routing in the recovery-outcome dispatch
     (required by the new `_RowOutcome` enum member).
   - `_maybe_hydrate_deferred_order`: pre-hydration `is_active_materialization_in_flight`
     check. Bypass: the poll-loop hydration consumer does NOT route through
     `PendingTriggerRestartRecovery` and can reselect an active #524 owner.
2. `ap_recovery.py`:
   - `_recover_deferred_breach_lifecycles`: pre-terminalization
     `is_active_materialization_in_flight` check. Bypass: this loop has
     independent 72h aging and terminal-lifecycle terminalization branches
     that do not consult PTR.
   - `MATERIALIZATION_OWNED` outcome routing in the two consumers of
     `PendingTriggerRestartRecovery.execute()` outcomes (required by the
     new enum member).

**REMOVED by amendment (partial-marker broadening):**

- All `ap/order_state_machine.py` edits from earlier commits on this branch
  (`_pending_entry_has_submit_or_recovery_owner` expansion, cleanup
  WHERE-clause additions in `cancel_pending_entry`/`expire_pending_entry`
  paths, retry CAS broker-marker additions, and the `_meta_blocks_hydration`
  helper with its four callsites in `record_deferred_hydration_result`).
- `ap/order_monitor.py` ghost-sweep SQL broadening at the EOD sweep.
- `ap_recovery.py` `broker_handoff_ambiguous_rows` bucket and the
  `_submit_intent_without_broker` gating on the pre-existing stale-pending
  and terminal-lifecycle branches. The base reconciler branch predicate
  (`meta.get("submit_intent_at") and not str(order.get("broker_order_id") or "").strip()`)
  is restored to its base form.

**Forbidden by amendment (never in this PR):**

- No new broker POST/cancel authority anywhere.
- No new permanent HOLD state for malformed materialization rows.
- No strategy, selection, spread, DTE, delta, OI, volume, premium, capital,
  sizing, entry, or exit policy changes.

The rest of this document is historical spec text preserved verbatim.

---

## Status

**SPEC FIRST / HARD HOLD / IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY THIS PR UNTIL CODE + PRODUCTION-SHAPED REGRESSION TESTS ARE PUSHED AND RE-AUDITED.**

Base when this spec branch was created:

```text
main = de87e3b0460e8908bc271590d425cc1fcfb9ae3d
```

That main includes merged PR #514. PR #520 is a separate open hotfix and owns deferred ExecutionCore claim ordering / duplicate callback work. This PR must remain a separate lifecycle/recovery correction with the smallest practical file surface.

---

## 1. Production incident this PR owns

On 2026-08-25 Jason LIVE produced a concrete race on TMO.

Identity:

```text
client_id      = jasoncosby1@gmail.com
execution_mode = live
ticker         = TMO
signal_id      = 2e0afecd-224c-415e-82a4-17494ac4acb1
local_order_id = d470966d-9fe1-4d30-b2d5-b7afbc8fb387
entry shape    = deferred / PENDING_TRIGGER
```

Observed ordering:

```text
13:39:27.905Z
WATCHER_TRIGGER_CALLBACK_ATTEMPT
  ticker=TMO
  attempt=1/3
  kept_in_pending=true

~2.5 seconds later, while breach-time selector/materialization work was still active:

13:39:30.446Z
DECISION_EVENT
  local_order_id=d470966d-9fe1-4d30-b2d5-b7afbc8fb387
  contract=DEFERRED:TMO
  decision=TERMINAL
  transition=PENDING_TRIGGER -> CANCELED
  last_error=pending_trigger_classifier:STUCK_TRIGGER_READY
```

The selector/materialization path continued after the local row had been canceled. TMO then emitted selector-quality failures and the watcher retained ownership/retried repeatedly.

The defect is therefore not "Tradier rejected TMO" and not "TMO had a wide spread." The order was terminalized locally by the pending-trigger recovery authority while another canonical component was still attempting breach-time materialization.

### Production race

```text
watcher confirms breach
        |
        v
ExecutionCore begins deferred materialization
        |
        |  materialization still in progress
        |
        +------------------------------+
                                       |
                                       v
                         pending-trigger recovery/classifier runs
                                       |
                                       v
                           watcher_reason=trigger_ready
                                       |
                                       v
                             STUCK_TRIGGER_READY
                                       |
                                       v
                         PENDING_TRIGGER -> CANCELED

Meanwhile selector/materializer continues underneath a terminal local row.
```

This violates lifecycle ownership even when every selector quality rule is correct.

---

## 2. Current-main code proof

### `ap/pending_trigger_classifier.py`

Current main classifies `trigger_ready` before it considers active materialization retry state.

Conceptually current ordering is:

```python
if watcher_reason == "trigger_ready":
    return STUCK_TRIGGER_READY

...

if retry_status in ("RETRY_PENDING", "RUNNING") or retry_next_at ...:
    return WAITING_RETRYABLE
```

So a durable row can truthfully contain both:

```text
watcher_audit.reason_code = trigger_ready
materialization_status    = RUNNING
lifecycle_state           = MATERIALIZING
materialization_in_flight = true
materialization_owner     = <valid owner>
materialization_generation= <current generation>
materialization_lease_until = <future>
```

and still be classified `STUCK_TRIGGER_READY` solely because the trigger-ready check wins first.

That precedence is the TMO bug.

### `ap/pending_trigger_restart_recovery.py`

Current restart/order-monitor recovery maps:

```text
STUCK_TRIGGER_READY
    -> _terminalize_with_reason(...)
    -> restart_stuck_trigger_ready_no_broker_proof
```

That terminalization is correct for a genuinely abandoned confirmed trigger with no active materialization authority. It is wrong for a currently owned, unexpired materialization attempt.

The correction must distinguish those two states without weakening the abandoned/zombie cleanup contract.

---

## 3. Relationship to adjacent PRs

### PR #445 — already merged

#445 owns recovery of genuinely stuck pre-broker `STUCK_TRIGGER_READY` rows when durable proof permits a safe recovery path. It does **not** solve the current race because TMO was being classified stuck while active work was still running.

Preserve #445's fail-closed behavior for genuinely abandoned rows.

### PR #514 — already merged

#514 owns deferred reservation vs actual selected contract cost sequencing and associated deferred materialization behavior.

Do not copy its capacity/risk/selector changes here.

### PR #520 — separate open P0 hotfix

#520 owns the ExecutionCore side:

```text
confirmed breach
-> durable materialization claim BEFORE capacity / sizing / selector / exposure work
-> duplicate callbacks lose at that claim boundary
```

This TMO PR owns the complementary recovery-side invariant:

```text
if that durable materialization claim is valid and still active,
pending-trigger recovery must recognize it and must not kill it.
```

**Do not merge #520's ExecutionCore changes into this PR.** Keep the PRs separately scoped.

Before implementation completion, rebase this PR onto the then-current main. If #520 has merged first, prove compatibility against the exact merged #520 durable row shape. If #520 has not merged, use its current branch only as read-only adjacency evidence and do not copy its production code.

### PR #519 — separate attempt/cursor authority work

#519 owns selector recovery attempt-counter/cursor authority and crash-safe retry identity. Do not solve that problem here.

This PR may read the canonical attempt/generation fields already written by existing code, but it must not redesign attempt counters or selector cursor semantics.

---

## 4. One binding invariant

> A `PENDING_TRIGGER` row with a **durably proven, current, unexpired deferred materialization owner** is not `STUCK_TRIGGER_READY` and must not be terminalized, canceled, recovery-rearmed, reselected, or have its attempt counters advanced by pending-trigger recovery.

The active materializer owns the attempt until it resolves the attempt or its durable lease expires.

Conversely:

> A row that merely claims to be materializing but cannot prove the current owner/generation/lease must receive no special protection. Existing fail-closed cleanup/recovery semantics remain authoritative.

---

## 5. Scope boundary / preferred file budget

### Production files expected

Prefer exactly these two production files:

```text
ap/pending_trigger_classifier.py
ap/pending_trigger_restart_recovery.py
```

### Test file expected

Prefer extending the existing P0 file already included in the authoritative P0 workflow:

```text
tests/test_p0_pending_trigger_restart_recovery.py
```

The implementation may instead extend `tests/test_p0_pending_trigger_lifecycle_integrity.py` if that produces a substantially cleaner production-shaped harness, but **do not touch both test files merely for convenience**.

### `ap/order_monitor.py`

Do **not** modify `ap/order_monitor.py` unless Codex first proves an actual caller bypass that cannot be corrected through the canonical restart-recovery authority. Current architecture says order monitor delegates PENDING_TRIGGER decisions into `PendingTriggerRestartRecovery`; fix the canonical authority rather than another caller.

If `ap/order_monitor.py` must change, stop and document:

1. exact bypass call path;
2. why the two canonical production files are insufficient;
3. exact additional mutation surface;
4. additional regression required.

### Explicitly out of scope

Do not change:

```text
ap_execution_core.py
ap_master_control.py
ap/contract_selector.py
ap/selector_retry_policy.py
ap/contract_quote_revalidator.py
broker submit adapters
broker cancel adapters
exit engine
position manager
proof_trades
scanner/ranking/intelligence
capital percentages
spread/OI/volume/delta/DTE/moneyness thresholds
```

No schema migration should be required. If Codex believes a schema change is necessary, stop and explain why the current durable materialization metadata cannot express this state before writing a migration.

---

## 6. Required classification model

Add one explicit nonterminal classification for a proven active materialization attempt. Preferred name:

```text
MATERIALIZATION_IN_FLIGHT
```

The exact constant name may differ only if an equivalent canonical state already exists on current main after rebase. Do not overload `WAITING_VALID`, `WAITING_RETRYABLE`, or `WATCHER_OWNED` if doing so hides the fact that the **materializer**, not an ordinary pre-breach watcher/retry, owns the row.

### Narrow precedence correction

The safest correction is narrow:

```text
PENDING_TRIGGER
+ no broker-owned/submitted evidence
+ watcher_reason=trigger_ready
+ explicit terminal materialization outcome absent
+ proven active materialization attempt
    -> MATERIALIZATION_IN_FLIGHT

PENDING_TRIGGER
+ watcher_reason=trigger_ready
+ active materialization proof absent/invalid/expired
    -> existing STUCK_TRIGGER_READY behavior
```

Do not broadly reorder unrelated invalidation, stop, EOD, or broker evidence policy unless a failing test proves a real conflict.

### Explicit terminal outcome wins

A contradictory row must not be protected simply because stale in-flight bits remain set.

If a canonical terminal materialization outcome is already durably present, it disqualifies `MATERIALIZATION_IN_FLIGHT` protection. Preserve terminal materialization truth.

---

## 7. Active materialization proof

Codex must first inventory the exact current-main writer(s) for these fields, especially after rebasing onto #520 if it has merged:

```text
lifecycle_state
materialization_status
materialization_in_flight
materialization_owner
materialization_generation
materialization_lease_until
retry_attempt
materialization_attempts
recovery_pre_claimed_attempt
client_id
execution_mode
local_order_id
signal_id
broker_order_id
submitted_ts
meta.submit_intent_at
```

Do not invent alternate metadata if current OSM already writes the required proof.

### Minimum proof predicate

For the TMO protection path, require at least:

```text
status == PENDING_TRIGGER
broker_order_id absent
submitted_ts absent
submit_intent/broker-ready contradiction absent

lifecycle_state == MATERIALIZING
materialization_status == RUNNING
materialization_in_flight is exactly true
materialization_owner is a non-empty canonical owner token
materialization_generation is a positive integer (bool is invalid)
materialization_lease_until is a parseable timezone-aware instant
materialization_lease_until > evaluation_time

no canonical terminal materialization outcome
```

`PendingTriggerRestartRecovery` already performs durable client/mode identity fencing before classification. Preserve and rely on those existing exact checks:

```text
row.client_id == recovery.client_id
row.execution_mode == recovery.execution_mode
local_order_id present
confirmed-trigger evidence identity proven where required
```

If current-main OSM claim identity also requires a canonical attempt number, include it in proof using the exact writer contract. Do not guess among `retry_attempt`, `materialization_attempts`, or other mirrors; trace the writer first.

### Lease handling

- Future, timezone-aware lease: may prove active ownership.
- Expired lease: does not prove active ownership.
- Missing lease: does not prove active ownership.
- Malformed lease: does not prove active ownership.
- Naive/no-timezone lease: fail closed; do not silently assume UTC unless the repository already has one canonical parser that explicitly defines that behavior.
- Far-future but otherwise valid lease is not by itself enough; every other owner/lifecycle/identity field must also agree.

For deterministic tests, prefer a small pure helper with an injectable/evaluable `now_utc` rather than patching global time throughout the suite.

---

## 8. Required recovery action

When classification returns `MATERIALIZATION_IN_FLIGHT`, `PendingTriggerRestartRecovery` must perform a **read-only preservation outcome**.

Preferred new row outcome:

```text
MATERIALIZATION_OWNED
```

If current summary machinery can represent an equivalent dedicated outcome without mutation, reuse it only if the semantics remain explicit in logs/tests.

### On active materialization recovery check, MUST NOT

```text
- terminalize the order
- transition PENDING_TRIGGER -> CANCELED/EXPIRED/ERROR/REJECTED
- recovery-rearm watcher ownership
- invoke entry_watcher.watch(... recovery_rearm=True)
- invoke selector/materializer
- invoke Master Control capacity/revalidation
- increment retry_attempt
- increment breach_attempt_count
- increment materialization_attempts
- change materialization_generation
- replace materialization_owner
- shorten/extend/renew the active lease
- write a new retry schedule
- call broker submit
- call broker cancel
- mutate positions
- mutate proof_trades
- mutate queue ownership/status
```

It should observe that another durable owner is active and return without stealing or "helping".

### Retry/check timing

Do not create a busy loop.

If the caller needs a next-check timestamp, it may use the already durable `materialization_lease_until` as an observational boundary. Pending-trigger recovery must not create a new lease or retry schedule merely because it noticed the owner.

A due retry after a legitimate materialization failure remains owned by existing deferred retry machinery, not this PR.

---

## 9. Exact TMO production-shaped regression

This is the primary acceptance test. Do not replace it with only isolated classifier unit tests.

Build one row/harness using the actual production identity shape:

```text
client_id      = jasoncosby1@gmail.com
execution_mode = live
ticker         = TMO
signal_id      = 2e0afecd-224c-415e-82a4-17494ac4acb1
local_order_id = d470966d-9fe1-4d30-b2d5-b7afbc8fb387
status         = PENDING_TRIGGER
contract       = DEFERRED:TMO
watcher_audit.reason_code = trigger_ready
```

Then model the point after a valid durable materialization claim but before selector completion:

```text
lifecycle_state            = MATERIALIZING
materialization_status     = RUNNING
materialization_in_flight  = true
materialization_owner      = materializer:<non-empty exact owner>
materialization_generation = 7  # or exact current writer shape
materialization_lease_until= now + 120s
retry/attempt identity      = exact current canonical writer value
broker_order_id             = null
submitted_ts                = null
```

Run the **real `PendingTriggerRestartRecovery` action path**, not just `classify_pending_trigger_row()`.

Assert:

```text
classification == MATERIALIZATION_IN_FLIGHT
recovery outcome == MATERIALIZATION_OWNED (or exact dedicated equivalent)

order status remains PENDING_TRIGGER
materialization owner unchanged
materialization generation unchanged
lease unchanged
attempt counters unchanged
client_id unchanged
execution_mode unchanged
local_order_id unchanged
signal_id unchanged

terminalize call count = 0
rearm call count        = 0
selector call count     = 0
capacity call count     = 0
exposure call count     = 0
broker submit count     = 0
broker cancel count     = 0
position mutation count = 0
proof mutation count    = 0
queue mutation count    = 0
```

### Concurrent/barrier version

Add one deterministic concurrency test if the existing harness supports it without introducing flaky sleeps:

```text
Thread/step A:
  durable materialization claim exists
  selector/materializer paused on Event/barrier

Thread/step B:
  PendingTriggerRestartRecovery examines same durable row

Expected:
  B returns MATERIALIZATION_OWNED/read-only
  row is not terminalized

Release A:
  A may continue through its ordinary existing completion/failure path
```

Use Events/barriers, never timing sleeps, so CI is deterministic.

If a real concurrent test would require an unreasonable amount of unrelated fixture machinery, a deterministic interleaving test that executes the exact same durable states in sequence is acceptable only if the PR body explicitly explains why and still exercises the real restart-recovery action path.

---

## 10. Mandatory negative-control matrix

Every case below must prove the row receives **no active-materialization protection** unless all required proof is valid.

At minimum:

1. `trigger_ready` + no materialization metadata -> existing `STUCK_TRIGGER_READY`.
2. `materialization_in_flight=false` -> not protected.
3. lifecycle state missing -> not protected.
4. lifecycle state not `MATERIALIZING` -> not protected.
5. materialization status missing -> not protected.
6. materialization status `RETRY_PENDING` -> existing retry behavior, not in-flight protection.
7. materialization status terminal -> terminal truth wins.
8. owner missing/blank -> not protected.
9. generation missing -> not protected.
10. generation zero/negative -> not protected.
11. generation boolean -> not protected.
12. lease missing -> not protected.
13. lease malformed -> not protected.
14. lease timezone-naive -> fail closed per canonical parser contract.
15. lease expired by 1 microsecond/second -> not protected.
16. explicit terminal materialization outcome + stale active flags -> terminal materialization wins.
17. wrong `client_id` vs recovery instance -> existing identity fence returns unresolved/fail-closed; no protection mutation.
18. wrong `execution_mode` -> existing identity fence returns unresolved/fail-closed; no protection mutation.
19. missing durable execution mode -> fail closed; never infer LIVE/PAPER.
20. missing local_order_id -> fail closed.
21. stale generation vs current durable generation where current writer exposes comparison -> stale owner cannot protect.
22. broker_order_id already present -> existing broker/adoption path owns; classifier does not call it active prebroker materialization.
23. submitted_ts/submit-intent evidence present -> existing broker-intent reconciliation path owns; no new prebroker protection.
24. non-PENDING_TRIGGER status -> existing NOT_PENDING behavior.
25. PAPER row with exact PAPER recovery context follows same lifecycle truth without being laundered into LIVE.
26. LIVE recovery cannot observe or protect PAPER materialization and vice versa.
27. another client cannot observe/protect this owner's row.

Do not weaken existing negative tests to make the new classification pass.

---

## 11. Existing behavior that must remain unchanged

### Genuine abandoned trigger-ready row

```text
PENDING_TRIGGER
watcher_reason=trigger_ready
no valid active materialization proof
no broker evidence
```

must continue through existing `STUCK_TRIGGER_READY` / #445-compatible fail-closed handling.

### Retry pending

A real retry-scheduled materialization should continue to use existing `WAITING_RETRYABLE` / deferred retry authority. This PR must not turn a due retry into permanent in-flight ownership.

### Terminal selector/materialization result

Canonical terminal outcome remains terminal even if stale owner/in-flight metadata was not perfectly cleared.

### Pre-breach waiting rows

`WAITING_VALID`, `WAITING_RETRYABLE`, orphan handling, EOD handling, continuation policy, stop/target invalidation, and rearm policy remain unchanged outside the exact active-materialization exception.

---

## 12. Broker and money-path safety

This PR adds no new broker authority.

For every new code path:

```text
broker submit calls = 0
broker cancel calls = 0
```

The PR prevents one incorrect local terminalization while another canonical owner is active. It does not authorize the active materializer to bypass any existing selector, risk, quote, submit, or OSM gate.

A materialization owner finishing successfully must still flow through existing durable OCC persistence, final risk checks, submit-time quote checks, and OSM broker submission authority.

A materialization owner finishing with `SPREAD_TOO_WIDE`, OI/volume failure, DTE/delta/moneyness failure, affordability failure, data-unavailable retry, or another canonical selector result must keep its existing taxonomy and terminal/retry behavior.

---

## 13. Absolutely no spread-policy change

This PR is lifecycle ownership only.

Do not modify:

```text
hard spread cap
SPREAD_TOO_WIDE classification
spread retry/terminal policy
OI threshold
volume threshold
delta range
DTE range
moneyness
premium limits
direct quote recovery policy
```

The TMO production order may still legitimately fail selection after the race is fixed. That is acceptable. The requirement is that it fails or succeeds **through the materializer that owns the attempt**, not because an unrelated cleanup path kills the row underneath it.

---

## 14. Forbidden fake fixes

Do not solve this by:

- adding a fixed sleep/grace period before `STUCK_TRIGGER_READY` cleanup;
- changing order-monitor poll frequency;
- extending PENDING_TRIGGER stale timeout;
- suppressing the STUCK log while still terminalizing;
- checking only `materialization_in_flight=true` without owner/generation/lease proof;
- accepting an expired or malformed lease;
- defaulting missing execution mode to LIVE or PAPER;
- defaulting missing client identity from runner context;
- converting every `trigger_ready` row into retryable;
- rearming the watcher while materialization is active;
- invoking selector again to "see if it is still valid";
- resetting attempt counters;
- resolving attempt-counter conflicts with `max()` or first-truthy logic;
- moving broker submit/cancel authority into recovery;
- deduplicating only at broker POST while allowing duplicate selector/lifecycle work;
- loosening spread/risk/quality gates so TMO happens to pass.

The fix is ownership truth, not delay, logging, or a wider trading gate.

---

## 15. Diagnostics requirements

Add one structured diagnostic for the preservation path. Preferred semantic shape:

```text
PENDING_TRIGGER_MATERIALIZATION_IN_FLIGHT
client_id=<exact>
execution_mode=<exact live|paper>
local_order_id=<exact>
signal_id=<exact>
materialization_owner=<exact/non-secret token>
materialization_generation=<int>
materialization_lease_until=<timestamp>
action=preserve_existing_materializer
broker_submission=NOT_ATTEMPTED
broker_cancel=NOT_ATTEMPTED
```

Do not call it `STUCK_TRIGGER_READY` in the same successful protection event.

Invalid/expired proof should retain existing fail-closed reason taxonomy and may include a narrow diagnostic explaining why active ownership was not proven. Diagnostics must never become control authority.

---

## 16. Test commands / regression expectation

Codex must run at minimum:

```bash
python -m pytest -q \
  tests/test_p0_pending_trigger_restart_recovery.py \
  tests/test_p0_pending_trigger_lifecycle_integrity.py \
  tests/test_p0_seam4_e2e_deferred_lifecycle.py \
  tests/test_p0_deferred_breach_lifecycle_completion.py \
  tests/test_p0_deferred_due_retry_ownership.py \
  tests/test_p0_fenced_retry_terminalization.py

python -m py_compile \
  ap/pending_trigger_classifier.py \
  ap/pending_trigger_restart_recovery.py

git diff --check
```

Then run the repository's authoritative exact-head P0 workflow.

If #520 is merged before implementation completes, rerun the #520 focused lifecycle tests as adjacency proof after rebasing.

No test is allowed to monkeypatch away the classifier/recovery seam this PR exists to fix.

---

## 17. Required fail-first proof

Before changing production code, add/reproduce one failing test on the rebased unmodified base that proves:

```text
trigger_ready + valid MATERIALIZING/RUNNING/in-flight owner + future lease
-> current classifier/recovery terminalizes as STUCK_TRIGGER_READY
```

Record the exact failing assertion in the PR completion report.

After implementation, the same fixture must pass with read-only `MATERIALIZATION_IN_FLIGHT` handling.

This prevents a test suite that merely validates the new implementation without proving the historical defect existed.

---

## 18. Acceptance criteria

This PR is not review-ready until all are true:

1. Exact TMO race is reproduced fail-first on rebased base.
2. Active, current, leased materialization wins over `trigger_ready` zombie classification.
3. Genuine abandoned `trigger_ready` rows remain `STUCK_TRIGGER_READY`.
4. Explicit terminal materialization outcome remains terminal.
5. Expired/malformed/missing ownership proof never protects a row.
6. Pending-trigger recovery performs zero terminal/rearm/selector/capacity/broker work on a proven active materializer.
7. Owner, generation, lease, attempts, client, mode, signal, and local order identity remain unchanged on the preservation path.
8. LIVE/PAPER and cross-client isolation are proven.
9. No broker submit/cancel authority added.
10. No positions/proof_trades/queue mutation added on preservation path.
11. No spread/risk/selector-quality threshold changed.
12. #445 genuine STUCK recovery semantics remain intact.
13. #519 attempt/cursor semantics are not redesigned.
14. Compatibility with #520 is proven after final rebase.
15. Focused tests green.
16. Adjacent lifecycle tests green.
17. Exact-head authoritative P0 CI green.
18. Final diff remains within the scoped file budget or every extra production file is separately justified.
19. Independent audit reviews the actual implementation diff before merge.

---

## 19. Codex implementation order

Codex should execute this exact sequence:

### Step A — rebase and inventory

1. Fetch latest `main`.
2. Rebase this branch.
3. Record exact base SHA.
4. Check whether #520 has merged.
5. Inventory current writers/readers of the active materialization fields listed in §7.
6. Confirm order monitor still delegates to `PendingTriggerRestartRecovery` and there is no bypass requiring `ap/order_monitor.py` edits.

### Step B — fail-first test

7. Add the TMO production-shaped fixture to the chosen existing P0 test file.
8. Run it against unmodified production code and capture the `STUCK_TRIGGER_READY` failure.

### Step C — smallest implementation

9. Add canonical active-materialization proof helper/classification in `ap/pending_trigger_classifier.py`.
10. Preserve explicit terminal materialization truth.
11. Add dedicated read-only action/outcome in `ap/pending_trigger_restart_recovery.py`.
12. Do not change any other runtime subsystem.

### Step D — negative controls

13. Add the full lease/owner/generation/identity negative-control matrix.
14. Add deterministic interleaving/concurrency proof if feasible without sleeps.

### Step E — regression/audit

15. Run focused + adjacent tests.
16. Run exact-head P0 CI.
17. Audit diff for broker, order, position, queue, proof, client/mode, selector, spread, and attempt-counter mutations.
18. Update PR body/comment with completion report.
19. Leave unmerged for independent review.

---

## 20. Completion report Codex must post

Before asking for merge review, post all of the following:

```text
BASE SHA:
HEAD SHA:

FILES CHANGED:
PRODUCTION FILES CHANGED:
TEST FILES CHANGED:

FAIL-FIRST RESULT:
EXACT TMO REPLAY RESULT:

CLASSIFIER BEFORE:
CLASSIFIER AFTER:

ACTIVE MATERIALIZATION PROOF FIELDS:
LEASE PARSER/AUTHORITY:
RECOVERY OUTCOME USED:

TERMINALIZE CALL COUNT ON ACTIVE OWNER:
REARM CALL COUNT ON ACTIVE OWNER:
SELECTOR CALL COUNT ON ACTIVE OWNER:
CAPACITY/REVALIDATION CALL COUNT ON ACTIVE OWNER:
BROKER SUBMIT COUNT ON ACTIVE OWNER:
BROKER CANCEL COUNT ON ACTIVE OWNER:

EXPIRED-LEASE NEGATIVE CONTROL:
MALFORMED-LEASE NEGATIVE CONTROL:
OWNER-MISSING NEGATIVE CONTROL:
GENERATION-INVALID NEGATIVE CONTROL:
CLIENT-MISMATCH NEGATIVE CONTROL:
MODE-MISMATCH NEGATIVE CONTROL:
TERMINAL-OUTCOME NEGATIVE CONTROL:
GENUINE-STUCK-TRIGGER POSITIVE TERMINAL CONTROL:

#445 COMPATIBILITY:
#520 COMPATIBILITY:
#519 NON-OVERLAP:

FOCUSED TEST RESULT:
ADJACENT TEST RESULT:
EXACT-HEAD P0 CI RESULT:

git diff main...HEAD --stat:

BROKER MUTATION AUDIT:
ORDER MUTATION AUDIT:
POSITION MUTATION AUDIT:
QUEUE MUTATION AUDIT:
PROOF_TRADES MUTATION AUDIT:
CLIENT_ID / EXECUTION_MODE AUDIT:
SELECTOR / SPREAD POLICY AUDIT:

FINAL VERDICT: MERGE / HOLD / HARD HOLD
```

Do not declare MERGE merely because local tests pass. The exact final diff and exact-head CI are required.

---

## 21. Merge gate

**HARD HOLD until implemented and independently re-audited.**

The implementation is acceptable only when the lifecycle becomes:

```text
confirmed breach
-> canonical durable materialization owner
-> pending-trigger recovery observes owner and stays read-only
-> owner alone resolves selector/materialization attempt
-> existing terminal/retry/broker handoff semantics continue
```

and never again:

```text
confirmed breach
-> selector working
-> cleanup path calls it STUCK_TRIGGER_READY
-> local order canceled underneath active materialization
```

## 22. Correction addendum (2026-08-27)

The audit found additional production consumers outside the original
classifier/restart-recovery diff that could bypass the fence. The correction
therefore extends the surgical scope to the actual bypasses:

1. The shared active-materialization predicate rejects broker-ready truth,
   submit intent/identity, and top-level or nested materialization outcomes;
   malformed metadata receives no protection.
2. Contradictory broker-ready or submit markers are held as ambiguous before
   restart recovery can quote, terminalize, rearm, or resubmit; generic OSM
   cancel/expire terminal CAS also reasserts the same ownership fences.
3. Restart recovery classifies the durable row before quote access and emits a
   structured `PENDING_TRIGGER_MATERIALIZATION_IN_FLIGHT` observation with
   `broker_submission=NOT_ATTEMPTED` and `broker_cancel=NOT_ATTEMPTED`.
4. Order-monitor deferred hydration and deferred-ghost expiry reassert the
   active-owner and broker-intent fences in both Python and SQL. The hydration
   copyback CAS has the same guards, including its compatibility fallback.
5. Startup deferred-lifecycle cleanup checks the active owner before stale or
   terminal cleanup, and both recovery consumers handle
   `MATERIALIZATION_OWNED` as read-only rather than unresolved/rearmable.

These added production files are required to preserve the Section 21 lifecycle
across every discovered caller; no broker, selector, position, queue, or
attempt-counter mutation is introduced for an active owner.
