# P0 — Recovery-rearm must not cancel an active deferred materializer

## Status

**SPEC ONLY / DRAFT / HARD HOLD / CODEX IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY.**

Base at creation:

```text
main@db4e3584b3319040b9e7bec519c26ce1b20a6c24
```

This is a surgical follow-up to merged PR #521. It does not replace #521, #606, #607, #612, or #613.

---

## 1. Production incident this PR owns

On 2026-09-11 Jason LIVE reproduced the same ownership family as the old TMO incident, but through a bypass that #521 never patched.

Exact UNH identity:

```text
ticker         = UNH
signal_id      = 834a02d9-1fe4-4c65-a0f1-236c80b41d3e
local_order_id = 402e3a0a-d452-4cd8-b519-b915af93eda9
execution_mode = live
contract       = DEFERRED:UNH
trigger         = 387.61 PUT
```

Observed ordering:

```text
13:41:49.842Z
UNH PUT CONFIRMED — bid=387.12 held below 387.61 for 2 polls

13:41:50.743Z
WATCHER_TRIGGER_CALLBACK_ATTEMPT

13:41:50.755Z
ExecutionCore: breach confirmed; submitting approved queued plan

13:41:50.838Z
DEFERRED_BREACH_CAPACITY_GATE_DEFERRED
reason=real_contract_cost_not_materialized

13:41:51.064Z
DEFERRED_SELECTOR_CAPACITY_RESOLVED decision=ALLOW

13:41:51.077Z
ExecutionCore begins breach-time contract selection/materialization

13:41:51.120Z
PENDING_TRIGGER -> CANCELED
last_error=pending_trigger_classifier:MATERIALIZATION_IN_FLIGHT

13:41:51.230Z
RECOVERY_REARM_BLOCKED
classification=MATERIALIZATION_IN_FLIGHT
watcher_owned=False
terminalized=true

13:41:53.787Z
selector: cursor exact-owner CAS missed

13:41:53.807Z
SELECTOR_RECOVERY_OWNERSHIP_LOST
```

The selector/materializer continued after recovery canceled its durable order. This is not a Tradier rejection and not a selector-quality problem. Recovery destroyed the row underneath a canonical active materializer.

---

## 2. Why merged #521 did not close this race

PR #521 added the canonical active-materializer proof and taught `PendingTriggerRestartRecovery` to return `MATERIALIZATION_OWNED` read-only.

Its production changed files were:

```text
ap/order_monitor.py
ap/pending_trigger_classifier.py
ap/pending_trigger_restart_recovery.py
ap_recovery.py
```

It did **not** modify `ap_entry_watcher.py`.

Current `main` therefore has two classification boundaries:

```text
PendingTriggerRestartRecovery.recover_one_row()
    -> classify_pending_trigger_row(...)
    -> active materializer => MATERIALIZATION_OWNED / read-only

BUT, if the first read happened before the materializer claimed ownership:

PendingTriggerRestartRecovery._rearm_and_verify()
    -> entry_watcher.watch(... recovery_rearm=True)
        -> reload durable order
        -> classify_pending_trigger_row(...)
        -> materializer may now be active
        -> MATERIALIZATION_IN_FLIGHT
        -> is_safe_to_recovery_rearm(...) == False
        -> _terminalize_recovery_rearm_candidate(...)
        -> PENDING_TRIGGER -> CANCELED
```

That second classifier is a time-of-check/time-of-use bypass around #521.

The race window is real:

```text
outer recovery classification: row is still rearmable
        |
        | concurrent watcher callback / ExecutionCore claim
        v
materializer becomes MATERIALIZING/RUNNING with valid owner+generation+lease
        |
        v
entry_watcher.watch(recovery_rearm=True) reloads row
        |
        v
inner classification = MATERIALIZATION_IN_FLIGHT
        |
        v
current main terminalizes it
```

The canonical classifier is correct. The consumer action is wrong.

---

## 3. Current-main code seam

### `ap_entry_watcher.py`

Inside `APEntryWatcher.watch()` for ordinary `recovery_rearm=True`:

```python
_recovery_row = self._load_order_row_for_recovery_rearm(local_order_id)
...
_recovery_classification = classify_pending_trigger_row(...)
...
if not is_safe_to_recovery_rearm(_recovery_classification):
    ...
    self._terminalize_recovery_rearm_candidate(...)
    return False
```

`MATERIALIZATION_IN_FLIGHT` is intentionally **not** safe to rearm. That does not mean it is safe to terminalize. It means another subsystem currently owns the row.

The method's own recovery-mode contract says recovery rearm is non-destructive and should not mutate the surviving order merely because watcher admission is rejected. The active-materializer branch violates that contract.

### `ap/pending_trigger_restart_recovery.py`

`_rearm_and_verify()` currently treats an ordinary `watch() -> False` as terminal unless the reject reason is the trigger-evidence identity fence:

```python
if not armed:
    if ... RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN ...:
        return UNRESOLVED

    return self._terminalize_with_reason(
        ...,
        "restart_recovery_watch_returned_false",
    )
```

Therefore changing only `APEntryWatcher.watch()` to return `False` without canceling is insufficient: the outer recovery caller would immediately terminalize the row anyway.

The handoff must be recognized at both ends.

---

## 4. Binding invariant

> If recovery rearm discovers a **canonically proven active deferred materializer** after the outer recovery check but before watcher admission, recovery must yield ownership to that materializer. It must not cancel, expire, rearm, reselect, replace ownership, advance attempts, or submit anything.

This is not permission to protect partial markers.

Protection remains exactly the existing #521 authority:

```python
is_active_materialization_in_flight(row)
```

Do not invent a second proof predicate.

Malformed, expired, stale, identity-conflicting, or partial materialization metadata receives no new authority from this PR.

---

## 5. Required production change — keep it to exactly two files

### File 1: `ap_entry_watcher.py`

Change only the ordinary `recovery_rearm=True` inner-classifier block.

After `_recovery_classification` is computed and before the generic `not is_safe_to_recovery_rearm(...)` terminalization branch:

```python
if (
    _recovery_classification
    == PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
):
    self._last_reject_reason = (
        PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT
    )
    # audit/log only
    return False
```

Requirements:

- zero `_terminalize_recovery_rearm_candidate()` calls;
- zero `cancel_pending_entry()` / `expire_pending_entry()` calls;
- zero watcher registration;
- zero selector/materializer calls;
- zero broker calls;
- zero order/position/proof/queue mutation except existing best-effort watcher audit metadata if already canonical for this branch;
- do not change `is_safe_to_recovery_rearm()`;
- do not classify active materialization as WAITING_VALID/WAITING_RETRYABLE;
- do not return `True` merely to avoid the cancel. `True` means watcher ownership exists and would trigger registry-proof semantics in the caller.

Use the existing per-call `_last_reject_reason` channel. The package wrapper already propagates the setter into the call result/shared result; no `ap_entry_watcher/__init__.py` change should be required.

### File 2: `ap/pending_trigger_restart_recovery.py`

In `_rearm_and_verify()`, before generic `watch returned False -> terminalize`:

```python
if getattr(watcher, "_last_reject_reason", None) == (
    PTC.MATERIALIZATION_IN_FLIGHT
):
    # Re-read/re-prove with the existing canonical predicate if practical.
    # If exact active ownership still exists:
    return _RowOutcome.MATERIALIZATION_OWNED

    # If it changed before reread, fail closed without destructive inference:
    return _RowOutcome.UNRESOLVED
```

Binding rules:

- Prefer re-reading the exact durable order and reusing `is_active_materialization_in_flight()` before returning `MATERIALIZATION_OWNED`.
- Never trust a stale reject string as durable ownership by itself.
- If reread fails, row disappears, identity changes, or active proof no longer validates, return `UNRESOLVED`; do **not** terminalize from this handoff.
- Do not create/renew a lease.
- Do not adopt the materializer owner token.
- Do not alter owner, generation, attempt counters, retry schedule, deadline, trigger authority, client, execution mode, signal identity, or contract.

No third production file is authorized by this spec. If Codex believes one is required, stop and leave the PR HARD HOLD with evidence.

---

## 6. Explicit non-scope / forbidden changes

Do not modify:

```text
ap/pending_trigger_classifier.py
ap/order_state_machine.py
ap_execution_core.py
ap_recovery.py
ap/order_monitor.py
ap/deferred_materializer.py
ap/selector_retry_policy.py
ap/contract_selector.py
ap_master_control.py
ap_lifecycle.py
ap_entry_watcher/__init__.py
broker adapters
reconciler
position manager
proof_trades
trade_queue
schema/migrations
```

Do not change:

- selector spread/OI/volume/delta/DTE/moneyness/premium policy;
- retry ceiling/backoff/deadline;
- trigger confirmation semantics;
- #606 trigger authority CAS;
- #607 canonical due-retry authority;
- #612 recovered pre-breach lifecycle restoration;
- #613 recovered durable trigger-ready re-ownership;
- LIVE/PAPER routing or identity normalization;
- broker submit/cancel/replace behavior.

No sleeps, grace periods, polling slowdowns, or timing heuristics.

---

## 7. Exact caller / authority chain after the fix

```text
APOrderMonitor / startup recovery
    -> PendingTriggerRestartRecovery.recover_one_row()
    -> initial classifier says rearmable
    -> _rearm_and_verify()
    -> APEntryWatcher.watch(recovery_rearm=True)

CONCURRENTLY:
watcher trigger callback
    -> ExecutionCore
    -> durable deferred-materialization claim
    -> materialization owner + generation + lease

BACK IN recovery watch:
    -> reload exact order
    -> classify_pending_trigger_row()
    -> MATERIALIZATION_IN_FLIGHT
    -> return False with exact per-call reason
    -> NO terminalization

BACK IN _rearm_and_verify():
    -> recognize exact reason
    -> durable reread / existing active-proof predicate
    -> MATERIALIZATION_OWNED (or UNRESOLVED if proof changed)
    -> NO mutation

EXISTING materializer alone continues:
    -> selector
    -> broker-ready copyback
    -> existing submit fence
    -> at most one broker submit
```

---

## 8. Required fail-first test

Create one focused P0 test file, preferably:

```text
tests/test_p0_recovery_rearm_materialization_handoff_race.py
```

Enroll it in the existing canonical P0 inventory.

The primary test must execute the real two-boundary race, not inspect source text.

Deterministic sequence using an Event/barrier or controlled row loader, never sleeps:

1. Seed production-shaped `PENDING_TRIGGER ENTRY` row with no active materializer.
2. Outer `PendingTriggerRestartRecovery` classifies it as rearmable and proceeds toward `_rearm_and_verify()`.
3. Before `APEntryWatcher.watch()` performs its durable recovery-row reload, mutate the same row into the exact valid #521 active-owner shape:
   - `lifecycle_state=MATERIALIZING`
   - `materialization_status=RUNNING`
   - `materialization_in_flight=true`
   - non-empty exact owner
   - positive integer generation
   - future timezone-aware lease
   - no terminal materialization outcome
   - exact client/mode/local-order/signal identity
   - no broker handoff contradiction.
4. Let real `APEntryWatcher.watch(recovery_rearm=True)` continue.

Unmodified `main@db4e3584...` must fail first by attempting terminalization / cancellation from the inner classifier.

Patched head must prove:

```text
final durable status = PENDING_TRIGGER
watcher added by recovery = false
recovery terminalize calls = 0
cancel_pending_entry calls = 0
expire_pending_entry calls = 0
materialization owner unchanged
generation unchanged
lease unchanged
attempt counters unchanged
trigger authority unchanged
client_id unchanged
execution_mode unchanged
signal/canonical identity unchanged
broker submit = 0 during recovery handoff
broker cancel = 0
position mutation = 0
proof mutation = 0
queue mutation = 0
```

Recovery outcome:

```text
MATERIALIZATION_OWNED
```

when the durable reread still proves the active owner, otherwise:

```text
UNRESOLVED
```

Never `TERMINALIZED`.

---

## 9. Exact UNH production replay

Add the September 11 identity as a named fixture/replay:

```text
ticker         = UNH
signal_id      = 834a02d9-1fe4-4c65-a0f1-236c80b41d3e
local_order_id = 402e3a0a-d452-4cd8-b519-b915af93eda9
execution_mode = live
trigger         = 387.61
contract        = DEFERRED:UNH
```

The replay does not need historical market data. The defect is authority timing, not market direction.

Prove the exact state transition that happened in production can no longer occur:

```text
MATERIALIZATION_IN_FLIGHT
    -X-> PENDING_TRIGGER -> CANCELED
```

---

## 10. Mandatory adversarial controls

At minimum:

1. active materializer appears between outer classification and inner watcher reload -> preserve owner.
2. active proof disappears before outer reread -> `UNRESOLVED`, no terminalization.
3. malformed/expired lease -> no false `MATERIALIZATION_OWNED` handoff.
4. owner blank -> no false handoff.
5. generation zero/negative/bool/string -> no false handoff.
6. client mismatch -> no cross-client protection.
7. LIVE/PAPER mismatch -> no cross-mode protection.
8. local-order or signal identity mismatch -> no protection.
9. terminal materialization outcome wins over stale active flags.
10. broker-order / submitted / submit-intent contradiction remains in existing broker authority path.
11. ordinary `WAITING_VALID` recovery still arms normally.
12. ordinary `WAITING_RETRYABLE` behavior remains unchanged.
13. true invalidated/stale/terminal cleanup behavior outside this active-owner race remains unchanged.
14. existing #613 recovered-trigger-ready branch remains unchanged.
15. stale `_last_reject_reason=MATERIALIZATION_IN_FLIGHT` without a currently valid durable active owner cannot return `MATERIALIZATION_OWNED`.
16. package-wrapper per-call result does not leak one concurrent watch result into another.

Add one positive money-path continuation control:

- after recovery yields to the active materializer, let that existing materializer complete through broker-ready handoff;
- assert **exactly one** existing broker submit on the valid path;
- assert recovery itself made zero submits/cancels.

At least one test must use a real disposable PostgreSQL orders row through the real reread/classification seam.

---

## 11. Existing tests that must stay green

At minimum run:

```text
tests/test_p0_pending_trigger_restart_recovery.py
tests/test_watch_recovery_rearm.py
tests/test_p0_reattach_no_cancel_on_missing_quote.py
tests/test_p0_live_recovered_watcher_lifecycle_msft.py
tests/test_p0_pr580_recovered_trigger_ready_recut.py
tests/test_p0_deferred_due_retry_ownership.py
tests/test_p0_deferred_breach_lifecycle_completion.py
```

The old #521 active-materializer tests are necessary but not sufficient. They test the outer canonical recovery path; this PR must test the nested `APEntryWatcher.watch()` reclassification race.

---

## 12. CI / merge gate

On one unchanged implementation SHA:

1. fail-first demonstrated on exact base;
2. focused new race test green;
3. old #521 TMO family green;
4. #606/#607/#612/#613 adjacency green;
5. real PostgreSQL race proof green;
6. `git diff --check` green;
7. exact-head canonical P0 green;
8. separate merge-ref canonical P0 green using the same inventory;
9. no production reroll between those results;
10. final net diff contains only the two authorized production files plus one focused test/spec/inventory line.

If `main` advances before implementation, rebase onto committed `main` and rerun fail-first. Do not absorb #614/#615 or unrelated open PR production code.

---

## 13. Money-path audit before merge

Must remain zero/new-none:

```text
new broker submit call sites
new broker cancel call sites
new broker replace call sites
new selector/materializer invocation paths
new position mutation paths
new proof_trades mutation paths
new queue mutation paths
new retry clocks
new ownership/lease writers
new LIVE/PAPER resolver
new trigger authority
```

The only intended live behavior change is:

```text
recovery discovers canonical MATERIALIZATION_IN_FLIGHT during watcher rearm
BEFORE: terminalize active PENDING_TRIGGER
AFTER:  yield read-only to canonical materializer
```

---

## 14. Codex implementation order

1. Read this spec and merged #521 spec.
2. Trace current `PendingTriggerRestartRecovery._rearm_and_verify()` -> `APEntryWatcher.watch(recovery_rearm=True)` exactly.
3. Add deterministic fail-first UNH race test before production changes.
4. Prove exact base cancels the row.
5. Patch the `MATERIALIZATION_IN_FLIGHT` branch in `ap_entry_watcher.py` only.
6. Patch the exact `watch() -> False` handoff in `ap/pending_trigger_restart_recovery.py` only.
7. Add durable reread/proof; fail closed to `UNRESOLVED` if ownership changed.
8. Run focused and adjacency suites.
9. Run exact-head and merge-ref P0 on unchanged head.
10. Stop. Do not continue into scheduler, lease, selector, or lifecycle redesign.

## Verdict

**HARD HOLD / DO NOT MERGE / DO NOT DEPLOY until implementation, fail-first, PostgreSQL race proof, exact-head + merge-ref P0, and independent final diff audit.**
