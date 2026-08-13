# P0 CURRENT-MAIN WORK ORDER — Recover `STUCK_TRIGGER_READY` before broker ownership

## Status

**DRAFT / HARD HOLD — SPEC ONLY. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This work order captures a 2026-08-12 LIVE entry-liveness defect in which valid, Master-Control-approved setups reached a durable trigger-ready/deferred state but were canceled without any broker order because pre-broker lifecycle ownership could not be continued or recovered safely.

The implementation must reproduce the exact current-main behavior before changing production code.

## Production evidence

Jason LIVE had several approved daily setups that never reached Tradier even though the underlying opportunity reached the pending-trigger lifecycle.

Observed examples included:

### HOOD — daily `3-1-2`

```text
status: PENDING_TRIGGER -> CANCELED
contract: DEFERRED:HOOD
broker_order_id: null
last_error: pending_trigger_classifier:STUCK_TRIGGER_READY
```

### BKNG — daily `1-2_2D`

```text
status: PENDING_TRIGGER -> CANCELED
contract: DEFERRED:BKNG
broker_order_id: null
last_error: pending_trigger_classifier:STUCK_TRIGGER_READY
```

### CL — daily `1-2_2D`

```text
status: PENDING_TRIGGER -> CANCELED
contract: DEFERRED:CL
broker_order_id: null
last_error: pending_trigger_classifier:STUCK_TRIGGER_READY
```

### CSX — daily `2-3`

```text
status: PENDING_TRIGGER -> CANCELED
contract: DEFERRED:CSX
broker_order_id: null
last_error: restart_stuck_trigger_ready_no_broker_proof
```

These are not ordinary broker rejects. They had **no broker ENTRY id and no broker submit proof**. The economic opportunity was lost inside Angel Precision's own pre-broker ownership/recovery path.

## Existing code contract

Current `ap/pending_trigger_classifier.py` already exists specifically to prevent zombie `PENDING_TRIGGER` rows and explicitly recognizes that trigger-ready rows whose callback failed can become stuck. Current restart recovery also has a dedicated `STUCK_TRIGGER_READY` path.

The defect is therefore not “add a classifier.” The classifier exists. The missing behavior is a safe, exact distinction between:

1. trigger-ready state with proven **zero broker ownership** that can resume the same logical pre-broker materialization opportunity; and
2. trigger-ready state with ambiguous or conflicting ownership that must HOLD/terminalize rather than risk duplicate submission.

## Non-overlap with existing PRs

This PR owns **pre-broker `PENDING_TRIGGER` continuation after durable trigger readiness when no broker order has been accepted**.

It must not duplicate:

- **#421** direction-reversal rearm. A genuine reversal/rebreach uses #421/#436 semantics, not this recovery path.
- **#422** strict selector recovery cursor validation. Malformed cursor authority still fails closed.
- **#429** canonical ENTRY position ownership after a broker fill. This PR ends before broker ownership/fill.
- **#430** post-cancel ENTRY retry. #430 owns entries after a broker order existed and was canceled.
- **#435/#436** breach intelligence and entry-efficiency waiting. Those may decide *whether/when* an opportunity is READY; this PR ensures an already-authorized pre-broker lifecycle does not disappear because ownership continuation broke.
- **#439** selector final-reason precedence.
- **#440** cancel/replace continuity after broker ownership.

No new post-cancel scheduler, selector retry counter, or watcher architecture may be invented here.

## Canonical invariant

For one exact logical ENTRY opportunity:

```text
confirmed durable trigger readiness
+ exact client/mode/signal/local-order ownership
+ proven zero broker ENTRY ownership
+ setup still valid
= recover the same pre-broker opportunity at most once
```

But:

```text
broker ownership ambiguous
OR owner identity ambiguous
OR durable trigger proof malformed
OR client/mode mismatch
= HOLD / fail closed, zero broker submit
```

`STUCK_TRIGGER_READY` must not mean either “always cancel” or “always retry.” It is a diagnosis requiring exact recovery proof.

## Required recovery dispositions

Reuse current taxonomy where possible. Conceptually the runtime must be able to distinguish:

- `STUCK_TRIGGER_READY_RECOVERABLE_NO_BROKER_PROOF`
- `STUCK_TRIGGER_READY_ALREADY_OWNED`
- `STUCK_TRIGGER_READY_BROKER_OWNERSHIP_AMBIGUOUS`
- `STUCK_TRIGGER_READY_TRIGGER_PROOF_INVALID`
- `STUCK_TRIGGER_READY_OWNER_CONFLICT`
- `STUCK_TRIGGER_READY_CLIENT_MODE_CONFLICT`
- `STUCK_TRIGGER_READY_SETUP_NO_LONGER_VALID`
- `STUCK_TRIGGER_READY_RETRY_EXHAUSTED`
- `STUCK_TRIGGER_READY_RECOVERED`

Names may follow existing project conventions. Do not turn a known recoverable no-broker condition into a generic `ORDER_CANCELED` with no explanation.

## What counts as proven zero broker ownership

Recovery may proceed only when current-main broker/OSM evidence proves the exact logical ENTRY has not crossed the broker ownership boundary.

At minimum inspect:

- exact `local_order_id`;
- `kind='ENTRY'`;
- exact `client_id`;
- exact normalized `execution_mode` (`live|paper` only);
- exact canonical `signal_id`/opportunity identity;
- durable `status=PENDING_TRIGGER` at the recovery boundary;
- `broker_order_id` missing/blank;
- `submitted_ts` missing;
- no durable submit-intent/broker-accepted evidence used by current OSM;
- no exact active broker ENTRY returned by any existing broker-truth recovery path if current architecture performs such lookup;
- no filled position for the exact economic identity;
- no conflicting descendant/replacement generation.

A missing broker id by itself is **not enough** if another submit-intent or ambiguous transport state says broker ownership could exist.

Any broker-read transport ambiguity must fail closed. Do not send a speculative duplicate order.

## Required implementation order

### 1. Reproduce HOOD/BKNG/CL/CSX on current main

Build production-shaped rows matching the 2026-08-12 incident. For each, prove:

- Master Control/setup phase already completed;
- exact deferred `PENDING_TRIGGER` ENTRY exists;
- trigger-ready evidence exists in the shape current main expects;
- no broker id / no submitted timestamp / no position;
- current classifier/restart path reaches `STUCK_TRIGGER_READY` and cancellation/terminal cleanup.

Record the exact call chain before patching.

### 2. Inventory current ownership authorities

Trace, without creating a second system:

- watcher in-memory ownership;
- durable watcher token/current owner;
- trigger-crossed timestamp and provenance;
- materialization generation;
- selector recovery cursor and counters;
- due-retry scheduling fields;
- local order lifecycle;
- OSM submit-intent/broker-owned evidence;
- restart reattachment path.

The implementation must appoint exactly one existing authority as the recovery owner.

### 3. Add a recoverable pre-broker continuation

When all exact no-broker and identity predicates pass, continue **the same logical local order/opportunity** instead of creating a replacement order.

The continuation must:

- retain the same `local_order_id` unless current OSM explicitly requires a descendant identity and that lineage is proven;
- retain exact client/mode/canonical signal;
- retain monotonic materialization/retry generation;
- preserve first breach provenance;
- preserve current direction/side;
- reacquire/verify watcher ownership through existing CAS/owner mechanisms;
- revalidate current setup truth before selector/broker work;
- call selector only through the current authorized deferred-materialization seam;
- pass through all current final submit gates.

### 4. Never revive a stale economic setup just because lifecycle recovery succeeded

Before resuming materialization, revalidate dynamic truth required by current main:

- current trigger relation / direction;
- target not already complete;
- stop/thesis not terminally invalid under current policy;
- session/time window;
- current account capital/risk;
- no conflicting entry/position;
- applicable #436 state when implemented;
- fresh contract selection/quality;
- final submit gates.

Recovery is permission to **re-evaluate**, not permission to buy.

### 5. Restart idempotency

A crash at every boundary must converge safely:

```text
before recovery claim
-> after recovery claim
-> before selector
-> after selector
-> before submit intent
-> after broker acceptance
```

On restart there must never be two independent recovery owners for the same exact opportunity.

If broker ownership appeared after the recovery claim, broker-owned lifecycle takes precedence and this pre-broker recovery becomes inert.

### 6. Duplicate callbacks/ticks

Concurrent health sweeps, watcher callbacks, or restart recovery workers must produce:

- at most one successful recovery claim;
- at most one selector/materialization continuation generation;
- at most one broker ENTRY POST if all final gates eventually authorize;
- zero second local ENTRY for the same logical opportunity unless canonical current architecture explicitly requires and proves descendant lineage.

## Expected production scope

Start with at most three current-main production files:

1. `ap/pending_trigger_classifier.py` — precise classification contract only.
2. `ap/pending_trigger_restart_recovery.py` — durable restart/recovery ownership.
3. `ap_execution_core.py` **or** the exact current materialization owner only if needed to resume the existing opportunity.

`ap/order_state_machine.py` is conditional. Do not alter it unless current durable CAS primitives cannot represent the required exact claim. If a fourth production file appears necessary, stop and document the dependency before expanding scope.

Do not rewrite `ap_entry_watcher.py` unless current-main tracing proves the watcher itself is the missing owner. Historical watcher architectures from reverted PRs are evidence only, not implementation templates.

## Required regression matrix

At minimum:

1. HOOD-shaped LIVE `STUCK_TRIGGER_READY`, exact trigger proof, no broker proof, valid setup -> recover same opportunity, no immediate duplicate submit.
2. BKNG-shaped case -> same.
3. CL-shaped case -> same.
4. CSX restart-shaped `restart_stuck_trigger_ready_no_broker_proof` -> same durable opportunity survives restart.
5. Recoverable case later finds a valid contract -> exactly one broker submit maximum.
6. Recoverable case later fails contract quality -> zero broker submit; remains/terminates according to existing selector taxonomy.
7. Recoverable case becomes unaffordable -> obey authoritative budget/reselection PR; zero submit until affordable.
8. Target completed before recovery continuation -> terminal, zero selector/broker continuation beyond required truth checks.
9. Direction reversed -> use canonical rearm path; old trigger-ready authority cannot submit.
10. Stop/thesis terminally invalid -> terminal, zero broker submit.
11. Missing trigger timestamp -> fail closed.
12. malformed trigger timestamp -> fail closed.
13. missing trigger provenance when provenance is required -> fail closed.
14. canonical signal mismatch -> fail closed.
15. client mismatch -> fail closed.
16. LIVE/PAPER mismatch -> fail closed.
17. NULL/blank/malformed execution mode -> fail closed.
18. broker id present -> this pre-broker recovery cannot run.
19. submitted timestamp present with no broker id -> ambiguous/broker-owned path; zero speculative POST.
20. submit-intent present -> zero pre-broker recovery submit.
21. exact broker lookup finds working ENTRY -> adopt existing broker lifecycle, zero duplicate POST.
22. broker lookup unavailable/transport error -> HOLD, zero POST.
23. matching position already exists -> do not create/revive ENTRY.
24. duplicate concurrent recovery workers -> one claim wins.
25. restart immediately after claim -> same generation; no reset to attempt zero.
26. restart after selector but before submit -> fresh final gates; no duplicate selector authority if durable state proves selection.
27. restart after broker acceptance -> broker lifecycle wins; recovery inert.
28. retry/materialization exhausted -> clean terminal, zero POST.
29. malformed/negative/conflicting retry counters -> preserve current strict fail-closed behavior.
30. zero new broker cancel path, zero position/proof/queue mutation before a real fill.

## Diagnostics

Every classification/recovery result must make it possible to answer:

- exact client/mode/signal/local order;
- trigger evidence and provenance status;
- durable owner token/generation;
- whether broker ownership was checked and what proved zero/ambiguous/owned;
- why recovery was permitted or denied;
- whether selector was invoked;
- whether broker submit was attempted;
- resulting durable state.

Avoid misleading `ORDER_CANCELED` as the only explanation for a pre-broker lifecycle failure.

## Money-path audit

Final implementation review must prove:

- **broker ENTRY POST:** zero on all failed/ambiguous recovery predicates; at most one after full successful continuation and final gates;
- **broker cancel:** no new cancel authority;
- **orders:** exact same logical ENTRY is preserved; no duplicate row on recovery;
- **positions:** zero mutation from this recovery before actual fill ownership;
- **proof_trades:** zero mutation;
- **trade_queue:** zero new writer;
- **client/mode:** exact/nonblank on every query/CAS;
- **PAPER/LIVE:** isolated;
- **Jason junk risk:** recovery does not bypass scanner, intelligence, selector quality, risk, or final-submit gates.

## Success criterion

A legitimate setup that reached trigger readiness but never crossed the broker ownership boundary must no longer be discarded merely because the callback/restart ownership seam became stuck.

The fix must improve **conversion liveness without duplicate-order risk**.

## Merge / deployment gate

**HARD HOLD** until:

1. exact current-main reproduction is attached;
2. all production-shaped cases above are implemented;
3. restart/concurrency tests use real PostgreSQL where required;
4. adjacent pending-trigger/watcher/selector/OSM P0 tests are green;
5. exact-head CI is green;
6. complete broker/mutation audit is attached;
7. independent whole-PR review is complete;
8. explicit merge authorization and separate deployment authorization are given.

No merge, deploy, environment change, migration application, or LIVE activation is authorized by this spec.