# P0 — Due materialization retry liveness after confirmed breach

**Status:** SPEC ONLY / HARD HOLD  
**Do not merge or deploy from this spec branch.**  
**Audited base:** `main@98eeaadae05f9e4e1db624703ec1e3cd758b732c`  
**Incident date:** 2026-09-08  
**Primary production client/mode:** Jason / LIVE

## 1. Executive summary

On September 8, 2026, Jason LIVE proved a post-breach liveness defect in the deferred-entry path. Valid deferred opportunities were discovered, armed, confirmed through their underlying trigger, and classified as retryable after temporary contract-market-data failure. Durable rows were written with `materialization_status=RETRY_PENDING`, but some rows did not resume their canonical materialization retry when `materialization_next_retry_at` became due.

Instead, the in-memory watcher continued confirming the same breach hundreds of times while the durable retry attempt remained stuck. Restart recovery simultaneously refused to take ownership because trigger evidence identity was not proven in the exact form it requires.

The result is a trade-flow suppression failure: the bot retains a live opportunity and repeatedly observes the breach, but the canonical selector/materialization attempt does not advance.

This PR must fix only that liveness/authority defect. It must not weaken selector quality, create a second retry scheduler, or add broker authority.

## 2. Production evidence

### 2.1 Jason LIVE MO

Local order:

`ce2d19be-be02-4af5-a9c1-130e1ada050e`

Observed durable shape later in the session:

- `status=PENDING_TRIGGER`
- `watcher_audit.reason_code=trigger_ready`
- `materialization_status=RETRY_PENDING`
- `materialization_outcome=RETRY_LATER_DATA_UNAVAILABLE`
- `materialization_next_retry_at=2026-09-08T14:03:32.996620+00:00`
- `retry_attempt=2`
- `breach_attempt_count=2`
- `materialization_attempts=2`
- watcher current owner remained present
- no broker order
- no submitted timestamp
- no position

The watcher continued confirming MO after the durable retry timestamp was already due. Production logs repeatedly showed:

```text
MO PUT CONFIRMED
-> NONE -> TRIGGER_READY illegal lifecycle diagnostic
-> WATCHER_TRIGGER_CALLBACK_ATTEMPT
-> Breach confirmed
-> KEEP_WATCHER
```

Restart recovery concurrently emitted:

```text
RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
```

and preserved the same row unchanged.

### 2.2 Jason LIVE MMM

Local order:

`3f16142b-81f6-4013-8db7-0cc727c6cfb5`

Observed durable shape:

- `status=PENDING_TRIGGER`
- `watcher_audit.reason_code=trigger_ready`
- `materialization_status=RETRY_PENDING`
- `materialization_outcome=RETRY_LATER_DATA_UNAVAILABLE`
- `materialization_next_retry_at=2026-09-08T13:41:01.146465+00:00`
- `retry_attempt=2`
- `breach_attempt_count=2`
- `materialization_attempts=2`
- watcher current owner remained present
- no broker order
- no submitted timestamp
- no position

MMM continued accumulating confirmed breach polls long after the durable retry timestamp was due.

### 2.3 Positive control: WFC

WFC demonstrated the expected liveness sequence on the same deployed SHA:

```text
DIRECT_QUOTE_ZERO_BID_ASK
-> DEFERRED_BREACH_SELECTOR_RETRYABLE
-> DEFERRED_MATERIALIZATION_RETRY
-> WATCHER_TRIGGER_OWNERSHIP_RETAINED disposition=RETRY_WAIT
-> retry becomes due
-> selector runs again
```

The fix must preserve this existing healthy retry path.

## 3. Prior known gap

The existing TMO pending-trigger/materialization spec documented that `trigger_ready` precedence over `materialization_status=RETRY_PENDING` was not fully resolved and left that case as separate follow-up work.

Current behavior can therefore produce a row carrying both:

```text
confirmed trigger evidence
+
canonical durable materialization retry authority
```

while classification/recovery paths disagree about which authority should win.

That disagreement is the defect this PR owns.

## 4. Scope ownership

### This PR owns

- canonical classification of `PENDING_TRIGGER + trigger-ready evidence + RETRY_PENDING`;
- due-vs-future-vs-exhausted durable retry authority;
- exact retry ownership verification across client/mode/signal/order/generation/attempt identity;
- liveness when `materialization_next_retry_at <= now`;
- making the existing canonical deferred materialization retry execute exactly once when due;
- preventing a valid durable retry from being misclassified as abandoned `STUCK_TRIGGER_READY`;
- preventing watcher-local behavior from becoming a second competing durable retry authority.

### This PR does NOT own

- PR #580 recovered lifecycle restoration (`NONE -> ADOPTED -> WATCHING`);
- PR #569 pre-open readiness / late watcher recovery freshness;
- selector scoring or contract-quality policy;
- spread limits;
- OI/volume thresholds;
- delta ranges;
- DTE playbook redesign;
- sizing redesign;
- account-capacity redesign;
- new broker submit authority;
- new broker cancel authority;
- position creation;
- proof-trade creation;
- exit logic;
- scanner behavior;
- sector identity;
- overnight attempt durability from PR #595.

## 5. Expected production files

Implementation should remain surgical. Expected primary files are:

- `ap/pending_trigger_classifier.py`
- `ap/pending_trigger_restart_recovery.py`
- tests covering those exact paths

`ap_execution_core.py` or `ap_entry_watcher.py` may be touched only if a fail-first behavioral replay proves the canonical due-retry consumer cannot be fixed without a narrowly scoped handoff change. Do not broaden merely for convenience.

## 6. Binding authority model

### 6.1 Active materializer wins

For an exact row with:

```text
status=PENDING_TRIGGER
lifecycle_state=MATERIALIZING
materialization_status=RUNNING
materialization_in_flight=true
exact materialization_owner
valid materialization_generation
valid lease
exact client/mode/signal/order identity
```

classification must remain:

`MATERIALIZATION_IN_FLIGHT`

The current materializer is the sole authority. No selector replay, watcher rearm, retry-counter increment, broker action, terminalization, or owner replacement may occur.

### 6.2 Valid future retry wins over STUCK_TRIGGER_READY

For:

```text
status=PENDING_TRIGGER
confirmed trigger evidence
materialization_status=RETRY_PENDING
valid exact retry identity
materialization_next_retry_at > now
```

classification must be:

`WAITING_RETRYABLE`

not `STUCK_TRIGGER_READY`.

The watcher may remain behavior-inert until the durable retry time, but it must not invent another durable retry timestamp or another retry attempt.

No broker call is permitted.

### 6.3 Due retry must become executable exactly once

For the same exact row when:

`materialization_next_retry_at <= now`

and:

- entry window remains valid;
- attempts remain according to canonical max-attempt authority;
- retry reason is retryable;
- exact owner/generation/attempt identity is proven;
- broker handoff evidence is absent;

then one canonical actor must atomically claim the retry and execute the existing deferred materialization retry path exactly once.

Required behavior:

```text
RETRY_PENDING due
-> exact CAS/ownership claim
-> RUNNING / in-flight authority for same generation contract
-> one selector/materialization attempt
-> one of:
   A. materialize real OCC contract and continue canonical submit path
   B. write one newer RETRY_PENDING attempt
   C. terminalize one honest terminal outcome
   D. HOLD/UNRESOLVED on ambiguous authority
```

There must never be two simultaneous selector attempts for the same exact retry generation/attempt.

### 6.4 Attempt counters are one authority family

The following durable aliases must never silently disagree:

- `retry_attempt`
- `breach_attempt_count`
- `materialization_attempts`

For canonical materialization retry rows, they must either:

- agree on the same integer attempt; or
- be resolved through a documented single canonical source if legacy compatibility requires it.

Missing, boolean, negative, malformed, whitespace, non-integer, or conflicting explicit values must HOLD / UNRESOLVED.

Do not coerce contradictory counters by `max()`, `min()`, truthiness, or last-writer-wins behavior.

### 6.5 Generation identity is monotonic and exact

A stale materialization generation must never execute a due retry.

Required proof includes the exact durable generation expected by the retry owner. A newer generation supersedes older retry authority.

Wrong or missing generation on a row that otherwise carries canonical retry markers must HOLD.

### 6.6 Retry timestamp authority

`materialization_next_retry_at` must be timezone-aware and parseable.

Cases:

- future timestamp -> wait;
- due timestamp -> eligible for exact claim;
- malformed explicit timestamp -> HOLD;
- missing timestamp while `RETRY_PENDING` -> HOLD;
- conflicting retry timestamp aliases -> HOLD;
- timestamp beyond entry deadline -> ordinary terminal/deadline authority, not indefinite wait.

Do not replace malformed durable truth with `now + delay`.

### 6.7 Retry reason authority

The retry reason must come from the existing canonical retry policy and preserve the actual selector failure.

Retryable data failure may retry.

Terminal quality/policy failures must not become retryable merely because the row also has `trigger_ready` evidence.

Do not broaden `RETRYABLE_BREACH_SELECTOR_REASONS` in this PR unless fail-first production evidence proves a taxonomy bug. The September 8 incident is primarily a liveness/ownership defect, not a request to loosen quality.

### 6.8 Broker handoff evidence fences the retry

Any of the following must block ordinary retry execution and route to the existing broker-intent/reconciliation authority:

- broker order id;
- submitted timestamp;
- submit intent;
- canonical broker submit key;
- broker submit payload hash;
- broker-ready proof where applicable;
- recovery submit owner/fence/lease;
- exact broker order tag found in authoritative current-session broker truth.

Unknown or incomplete broker truth must not be converted into broker absence.

## 7. Watcher interaction contract

The watcher is allowed to retain ownership only as required to prevent ownerless opportunities. It must not become a parallel retry scheduler.

For a durable `RETRY_PENDING` row:

- the durable `materialization_next_retry_at` is authoritative;
- an in-memory five-second `KEEP_WATCHER` cadence may poll/observe, but must not overwrite or mask the durable retry schedule;
- when due, the canonical retry path must own progression;
- repeated trigger confirmation must not reset the durable attempt counter;
- repeated trigger confirmation must not write a newer fake retry time unless the canonical retry attempt actually runs and fails transiently.

A due row must not remain indefinitely in:

```text
PENDING_TRIGGER
+ trigger_ready
+ RETRY_PENDING
+ next_retry_at in the past
+ unchanged attempt counters
```

while poll count continues to grow.

## 8. Required fail-first reproductions

### 8.1 MO production shape

Build a production-shaped PostgreSQL row matching the September 8 MO state:

- LIVE;
- exact client;
- deferred contract;
- PENDING_TRIGGER;
- confirmed trigger evidence;
- RETRY_PENDING;
- attempt aliases all `2`;
- materialization generation present;
- due `materialization_next_retry_at`;
- no broker order;
- no submit intent;
- entry window valid.

On current main, prove the row fails to progress or is misclassified.

After fix, prove exactly one canonical retry attempt occurs.

### 8.2 MMM production shape

Repeat with the MMM durable shape and prove the same invariant independently.

### 8.3 WFC positive control

Preserve the healthy sequence where an ordinary transient quote miss writes retry attempt 1 and later progresses into retry attempt 2.

## 9. Required behavioral/adversarial test matrix

### Classification

1. `trigger_ready + valid future RETRY_PENDING` -> `WAITING_RETRYABLE`.
2. `trigger_ready + active RUNNING materializer` -> `MATERIALIZATION_IN_FLIGHT`.
3. `trigger_ready + no retry/no materializer/no broker proof` -> existing `STUCK_TRIGGER_READY` behavior remains.
4. terminal materialization outcome remains terminal.
5. watcher invalidation remains terminal.

### Due retry

6. due retry + exact authority -> exactly one canonical claim.
7. two concurrent recovery workers -> one wins, one performs zero selector/broker work.
8. watcher callback racing due-retry worker -> one canonical materialization attempt.
9. process death immediately after claim -> restart recognizes current owner/lease and does not duplicate work.
10. process death after selector but before retry persistence -> no duplicate submit, safe restart classification.
11. process death after retry persistence -> next generation/attempt remains durable and idempotent.

### Identity

12. wrong client -> HOLD, zero mutation.
13. wrong execution mode -> HOLD, zero mutation.
14. wrong signal id -> HOLD.
15. wrong local order id -> HOLD.
16. stale generation -> HOLD.
17. newer generation supersedes older attempt.
18. missing generation on canonical retry row -> HOLD.

### Counter corruption

19. `(2,2,2)` accepted.
20. `(2,3,2)` HOLD.
21. explicit zero when zero is invalid -> HOLD.
22. negative -> HOLD.
23. boolean -> HOLD.
24. numeric string only if the existing canonical schema explicitly allows it; otherwise HOLD.
25. whitespace -> HOLD.

### Timestamp corruption

26. future timestamp waits.
27. due timestamp runs.
28. malformed explicit timestamp HOLDs.
29. naive timestamp HOLDs unless canonical parser explicitly normalizes by documented contract.
30. missing timestamp while RETRY_PENDING HOLDs.
31. deadline already expired -> terminal/deadline path, no selector.

### Broker safety

32. broker order id present -> zero selector, zero submit.
33. submit intent present -> zero ordinary retry submit.
34. authoritative broker tag found -> reconcile/HOLD, no duplicate broker POST.
35. broker truth UNKNOWN -> HOLD, never assume absence.

### Money-path safety

36. zero position mutation before confirmed fill.
37. zero proof_trades before confirmed fill.
38. zero queue/result corruption.
39. exactly one submit on a valid fully materialized path.
40. no new cancel authority.

### Liveness

41. due retry cannot remain unchanged while watcher poll count increases.
42. successful retry closes the retry state correctly.
43. transient retry failure writes exactly one next attempt and one newer next_retry_at.
44. terminal quality failure terminalizes exactly once.
45. restart replay is idempotent.

## 10. Production-path trace required in PR evidence

The implementation PR must document the exact call graph for the fixed row:

```text
watcher confirms breach
-> callback durable disposition verification
-> pending-trigger classification
-> retry ownership verification
-> due-time decision
-> retry claim CAS
-> deferred materialization resume
-> selector
-> contract copyback
-> broker handoff OR retry persist OR terminal outcome
```

For every changed production function, explicitly identify:

- caller;
- state read;
- authority source;
- identity validation;
- mutation;
- downstream consumer;
- return/disposition classification.

## 11. No-regression constraints

Do not:

- weaken the 12% hard spread gate;
- lower liquidity gates;
- bypass earnings lockout;
- bypass account-size limits;
- create synthetic contract symbols;
- submit a `DEFERRED:*` contract;
- use stale option quotes as executable truth;
- add a second materialization generation counter;
- add a second retry ownership model;
- infer LIVE from missing execution mode;
- move retry authority into watcher-local memory;
- add broker cancel behavior.

## 12. CI / release gate

The implementation PR remains HARD HOLD until all are true:

1. Rebase onto the current post-dependency `main` at implementation time.
2. Diff remains limited to this failure class.
3. Production-shaped PostgreSQL fail-first replay demonstrates the current bug.
4. MO and MMM shaped behavioral tests pass after the fix.
5. WFC positive-control retry still passes.
6. Exact-head P0 suite is green at the attested HEAD SHA.
7. `pull_request` merge-ref suite runs the same valid P0 test list and is green.
8. No new broker submit/cancel authority exists outside the existing canonical path.
9. Independent backwards audit finds no trade-flow regression.
10. PR remains draft until explicitly cleared.

## 13. Merge verdict

**HARD HOLD.**

This spec authorizes implementation and testing only. It does not authorize merge or deployment.
