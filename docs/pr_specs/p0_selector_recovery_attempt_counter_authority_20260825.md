# P0 SPEC — Repair deferred selector recovery attempt-counter authority conflicts

**Status:** SPEC ONLY / HARD HOLD / CODEX IMPLEMENTATION REQUIRED

**Base main SHA:** `be8e4d484ac1c69dc6e88e2885c9779420e97f5c`

**Production date:** 2026-08-25

**Severity:** P0 tradeflow correctness + retry ownership + broker-safety invariant

---

## 1. Executive summary

A real Jason LIVE deferred entry reached the selector-recovery retry path and was terminalized with:

```text
SELECTOR_RECOVERY_CURSOR_INVALID:MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT
```

The durable `orders.meta` row proves the conflict was not hypothetical:

```text
symbol                    = MCD
side                      = PUT
execution_mode            = live
status                    = EXPIRED
local_order_id            = 8767886d-7619-4b8f-a87f-f9a2821c39c0
signal_id                 = 73ad6808-cbf0-4218-afb5-d960a613baca
retry_attempt             = 2
breach_attempt_count      = 1
materialization_attempts  = 1
recovery_pre_claimed_attempt = null
selector_recovery_cursor  = null
materialization_outcome   = RETRY_LATER_DATA_UNAVAILABLE
retry_reason              = DIRECT_QUOTE_ZERO_BID_ASK
broker_order_id           = null
submitted_ts              = null
filled_ts                 = null
```

The bot correctly failed closed once it detected contradictory retry state. The bug is **not** that `_resolve_selector_attempt_number()` rejects disagreement. The bug is that production writers allowed fields that the resolver treats as one durable attempt identity to diverge.

This PR must repair the durable authority model and every writer/handoff/restart seam that can create this state. It must **not** make the resolver permissive, guess which number is right, take `max()`, silently rewrite a corrupt row, or let broker execution continue under ambiguous retry identity.

The correct result is one monotonic, race-safe selector-materialization attempt authority whose mirrors, if retained, cannot diverge under normal execution, process crash, restart recovery, watcher callbacks, retry scheduling, or concurrent workers.

---

## 2. Why this is P0

This seam sits directly between a confirmed deferred entry and the breach-time contract selector.

Failure has two bad directions:

1. **Too strict because state is internally inconsistent:** a legitimate trade is lost before selector recovery can continue, as happened to MCD.
2. **Too permissive if fixed incorrectly:** two workers can believe they own the same selector attempt, invoke selector twice, advance the cursor inconsistently, or eventually race toward broker submission.

Therefore the implementation must improve liveness **without weakening the existing fail-closed broker-safety properties**.

This is not an observability-only PR. It changes retry lifecycle behavior when the durable state is valid. It must remain fail closed when the durable state is genuinely contradictory or cannot be proven safe.

---

## 3. Production incident: exact evidence

Read-only Supabase evidence for Jason LIVE on 2026-08-25:

```text
local_order_id:           8767886d-7619-4b8f-a87f-f9a2821c39c0
signal_id:                73ad6808-cbf0-4218-afb5-d960a613baca
symbol:                   MCD
direction:                PUT
status:                   EXPIRED
last_error:               SELECTOR_RECOVERY_CURSOR_INVALID:MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT
contract_selection_status: null
broker_order_id:          null
submitted_ts:             null
filled_ts:                null
created_ts:               2026-08-25 13:33:34.574754+00
updated_ts:               2026-08-25 13:45:07.765766+00
retry_attempt:            2
breach_attempt_count:     1
materialization_attempts: 1
recovery_pre_claimed_attempt: null
materialization_outcome:  RETRY_LATER_DATA_UNAVAILABLE
materialization_reason:   SELECTOR_RECOVERY_CURSOR_INVALID:MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT
retry_reason:             DIRECT_QUOTE_ZERO_BID_ASK
selector_recovery_cursor: null
```

The previous selector/materialization attempt had a retryable market-data reason, `DIRECT_QUOTE_ZERO_BID_ASK`. Attempt 2 was then represented durably by `retry_attempt=2`, while the other attempt mirrors still said 1.

The system terminalized rather than guessing. That broker-safe behavior is correct and must be preserved for truly unresolvable corruption.

---

## 4. Current authority model that Codex must understand before editing

### 4.1 `_resolve_selector_attempt_number()` is intentionally strict

Current `ap_execution_core.py` resolves selector attempt identity from:

```text
retry_attempt
breach_attempt_count
materialization_attempts
recovery_pre_claimed_attempt
```

The current documented contract distinguishes:

- `retry_attempt`, `breach_attempt_count`, `materialization_attempts` as durable/canonical retry counters expected to agree;
- `recovery_pre_claimed_attempt` as an in-process claim signal that may legitimately be ahead during a transactional handoff.

The resolver rejects malformed or contradictory durable counters with:

```text
MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT
```

Do **not** remove this invariant as a shortcut.

### 4.2 OSM has a writer that intentionally writes all three counters together

`ap/order_state_machine.py::schedule_deferred_materialization_retry(...)` currently writes the same `_attempt` into:

```text
retry_attempt
breach_attempt_count
materialization_attempts
```

inside the same retry lifecycle patch, along with retry schedule and ownership metadata.

This is strong evidence that these fields are intended to describe one retry attempt at that boundary.

### 4.3 OSM also has a pre-claim path that advances `retry_attempt`

Current OSM code contains a CAS claim path whose own comment says it advances canonical `retry_attempt` atomically with materialization generation **before** the later retry scheduling call, so a process crash does not reuse an attempt slot.

That path also writes `retry_attempt_in_flight` as a diagnostic alias and predicates the claim on the previous canonical attempt.

This creates a critical handoff state that Codex must model explicitly:

```text
prior durable attempt = 1
claim attempt 2 succeeds
retry_attempt becomes 2
other mirrors may still be 1 until later lifecycle persistence
```

That intermediate state may be legitimate **only while ownership/provenance proves the attempt-2 claim**. It cannot be treated as generic healthy persistent state after context is lost.

### 4.4 Legacy/current helper paths can write `materialization_attempts` independently

`ap/deferred_materializer.py` contains lifecycle helpers such as:

```text
stamp_trigger_queued()    -> materialization_attempts = 0
stamp_running()           -> materialization_attempts = attempt
stamp_selected()          -> materialization_attempts = attempt
stamp_retry_pending()     -> materialization_attempts = attempt
```

These helpers do not inherently mirror `retry_attempt` and `breach_attempt_count` in the same write.

Codex must determine which of these functions are still reachable in the production deferred-entry lifecycle. Do not assume a file is dead merely because newer OSM helpers exist. Prove call reachability from current runner/watcher/recovery paths.

### 4.5 Restart recovery still treats `materialization_attempts` as a canonical retry field

`ap/pending_trigger_restart_recovery.py` explicitly names `materialization_attempts` as the canonical attempt counter for #323-era materialization retry metadata.

The repository therefore currently contains overlapping generations of retry metadata contracts. The implementation must reconcile current production authority rather than layering another alias on top.

### 4.6 Deployment preflight deliberately passes no transient preclaim context

`ap/selector_recovery_deploy_preflight.py` calls the production resolver with:

```python
recovery_pre_claimed_attempt=None
```

when evaluating durable rows. This is appropriate because a deployment preflight cannot trust an in-memory claim from a dead/restarted process.

A safe design must therefore leave rows in a durable state that is independently interpretable after process death. It is not acceptable for correctness to depend on a transient Python variable surviving long enough to explain why one persisted counter is ahead.

---

## 5. Root-cause hypothesis to prove, not blindly assume

The production MCD shape strongly suggests this sequence is possible:

```text
attempt 1 fails with retryable selector-data reason
-> retry is durably scheduled at attempt 1
-> due-retry worker claims the next attempt
-> OSM CAS advances retry_attempt from 1 to 2 to prevent slot reuse
-> breach_attempt_count/materialization_attempts remain 1
-> transient recovery_pre_claimed_attempt context is absent/lost/not propagated
-> selector attempt resolver rereads the durable row
-> sees 2 / 1 / 1
-> fails closed with MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT
```

That is a hypothesis. Codex must trace every real call and writer before implementing.

The true defect may instead involve a different writer, partial lifecycle migration, restart rehydration, stale row snapshots, or a call path invoking the resolver after a legitimate preclaim without passing the claim proof. The PR must fix the proven authority seam, not merely this narrative.

---

## 6. Mandatory writer inventory before code changes

Codex must identify **every production writer and reader** for all of the following fields:

```text
retry_attempt
retry_attempt_in_flight
breach_attempt_count
materialization_attempts
recovery_pre_claimed_attempt
materialization_generation
materialization_owner
current_owner
materialization_in_flight
materialization_status
lifecycle_state
next_retry_at
materialization_next_retry_at
selector_recovery_cursor_v1
watcher_token
recovery_owner
```

At minimum inspect:

```text
ap_execution_core.py
ap/order_state_machine.py
ap/deferred_materializer.py
ap/pending_trigger_restart_recovery.py
ap/selector_recovery_deploy_preflight.py
ap_entry_watcher.py
ap_entry_watcher/__init__.py
ap/order_monitor.py
ap_recovery.py
client_runner.py
```

and all code-search hits/tests that exercise deferred selector recovery.

For each writer, document in the PR implementation notes:

- function/method name;
- whether it is reachable in LIVE;
- lifecycle state required before write;
- fields changed;
- CAS predicates / row identity predicates;
- whether it can run concurrently with watcher/recovery worker;
- whether it can survive/recover after process crash;
- whether it can advance attempt identity;
- whether it can clear/reset attempt identity;
- whether it writes a cursor;
- whether it can reach selector;
- whether it can reach broker submit directly or indirectly.

Do not merge until the inventory explains how the exact production `2 / 1 / 1` state was created or demonstrates a reproducible equivalent race/crash window.

---

## 7. Required canonical authority model

The implementation must establish one authoritative monotonic selector-materialization attempt identity.

There are two acceptable high-level designs.

### Option A — one canonical field, mirrors become derived/observability-only

Example:

```text
retry_attempt = sole durable attempt authority
```

Other fields may remain for compatibility but must either:

- be derived from the canonical value at read time;
- be updated atomically with it where retained;
- or be explicitly classified as historical/non-authoritative and removed from strict attempt resolution.

This option is acceptable only if every reader is migrated safely and older valid rows remain interpretable without guessing.

### Option B — retain mirrored canonical fields but make them transactionally indivisible

If all three remain authoritative, every transition that advances attempt identity must update:

```text
retry_attempt
breach_attempt_count
materialization_attempts
```

in the **same fenced durable CAS**.

There must be no normal persistent lifecycle state where one is 2 and the others are 1.

If a transient preclaim requires an intermediate state, encode that state explicitly and durably with sufficient owner/generation/provenance to distinguish it from corruption. Do not rely solely on an in-memory `recovery_pre_claimed_attempt` value.

### Strong preference

Prefer the smallest design that removes split authority rather than adding a fourth/fifth counter alias. The codebase has enough metadata archaeology already.

---

## 8. Non-negotiable attempt invariants

### 8.1 Monotonicity

For one exact deferred order generation:

```text
attempt N -> attempt N+1
```

Attempt identity must never decrease.

### 8.2 Uniqueness

At most one active owner may claim a given:

```text
(client_id, execution_mode, local_order_id, materialization_generation, attempt)
```

### 8.3 No attempt reuse

A crashed or expired worker must not cause the same consumed attempt number to be used again if selector work may already have run.

### 8.4 No unproven attempt skipping

Do not blindly jump from 1 to `max(counter_values)` or to 3 because conflicting metadata exists. Advancing through corruption without proof can skip cursor state and conceal duplicate work.

### 8.5 Durable restart interpretability

After a process dies at any instruction boundary, a new process must be able to classify the row from database state alone into one of:

```text
safe to resume same attempt
safe to claim next attempt
retry not yet due
already owned by a live/current claim
terminal
broker ready
corrupt / unresolved -> fail closed
```

### 8.6 Cursor consistency

Attempt 2+ must continue to obey existing cursor requirements.

A repair must not create attempt 2 and then silently permit a missing/invalid cursor merely to restore tradeflow.

If the correct recovery path needs a cursor and no valid identity-bound cursor exists, remain fail closed or return to a state where attempt 1 may be legitimately recomputed only if the lifecycle contract explicitly proves no prior selector attempt executed. Do not infer that from missing data alone.

### 8.7 Identity fencing

Every repair/claim/update must preserve and predicate on exact:

```text
client_id
execution_mode
local_order_id
signal_id / canonical identity where required
materialization_generation
owner/token where required
current lifecycle state
```

No cross-client or LIVE/PAPER recovery is permitted.

---

## 9. Required race and crash matrix

Codex must implement tests for all relevant orderings below.

### A. Watcher callback vs due-retry worker

- watcher sees already-confirmed breach while row is `RETRY_WAIT`;
- due-retry worker claims next attempt;
- watcher fires again before selector begins;
- exactly one owner continues;
- loser does not increment counters or invoke selector.

### B. Two due-retry workers

- both read attempt 1 as due;
- both try to claim attempt 2;
- exactly one CAS succeeds;
- exactly one attempt-2 selector invocation;
- loser rereads and observes current owner/state.

### C. Crash after claim CAS but before selector call

This is the most important candidate for the MCD shape.

Required:

- durable state after crash is self-describing;
- restart does not classify it as arbitrary counter corruption if it is a legitimate claimed attempt;
- stale claim expiry/recovery uses generation/lease fencing;
- the same attempt is not double-used if proof says selector could have started;
- broker remains unreachable.

### D. Crash after selector call begins but before result persistence

Required:

- do not assume selector did not run;
- do not reuse the same attempt without explicit idempotence proof;
- cursor/attempt semantics remain safe;
- no broker double-submit can emerge later.

### E. Crash after retryable selector result but before retry schedule persistence

Required:

- restart cannot produce an infinite watcher-driven loop;
- lifecycle either completes the prior attempt result or fail-closes to unresolved repair-required state;
- no fabricated cursor.

### F. Crash after retry schedule persistence but before worker sleeps/exits

Required:

- retry executes only when due;
- watcher does not bypass the durable schedule;
- next claim increments exactly once.

### G. Restart with stale owner token

Required:

- expired/stale owner cannot mutate a newer generation;
- recovery may clear/reclaim only with exact lease/generation proof.

### H. Direction reversal / rearm

Current OSM direction-reversal reset explicitly clears retry counters to 0 while preserving monotonic materialization generation.

Required:

- reset is allowed only under the existing proven direction-reversal CAS;
- old cursor/attempt state cannot bleed into the new direction;
- stale old-direction worker cannot advance the reset generation.

### I. Broker-ready race

If another worker advances row to broker-ready/selected while retry recovery is evaluating stale metadata:

- recovery CAS must fail;
- no retry counter rewrite may clobber broker-ready state;
- no second selector run;
- no duplicate broker submit.

---

## 10. Required production repair behavior for the exact MCD shape

The implementation must include a deterministic fixture matching:

```json
{
  "client_id": "jasoncosby1@gmail.com",
  "execution_mode": "live",
  "local_order_id": "8767886d-7619-4b8f-a87f-f9a2821c39c0",
  "signal_id": "73ad6808-cbf0-4218-afb5-d960a613baca",
  "symbol": "MCD",
  "direction": "PUT",
  "status": "PENDING_TRIGGER or exact pre-terminal replay state",
  "meta": {
    "retry_attempt": 2,
    "breach_attempt_count": 1,
    "materialization_attempts": 1,
    "recovery_pre_claimed_attempt": null,
    "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
    "retry_reason": "DIRECT_QUOTE_ZERO_BID_ASK"
  }
}
```

Important: the production row is already terminal (`EXPIRED`), so tests must reconstruct the immediate pre-terminal lifecycle state from logs/code rather than mutating production or pretending an EXPIRED row is resumable.

The implementation must demonstrate which earlier durable state produced `2/1/1`, then prove that **newly-created rows cannot reach that split state** under the same sequence.

Do not add an automatic database migration that rewrites historical EXPIRED rows back to live eligibility.

---

## 11. Runtime repair policy for already-in-flight nonterminal rows

If this implementation encounters a nonterminal row with conflicting counters after deployment, it needs an explicit policy.

### Safe automatic repair is allowed only with proof

Automatic convergence may occur only if the code can prove, from durable state, all of:

- exact `client_id` and `execution_mode` match the current runner;
- exact `local_order_id` matches;
- row is still pre-submit (`broker_order_id` absent, `submitted_ts` null, `broker_ready` false);
- lifecycle state is one of an explicitly reviewed deferred-recovery states;
- materialization generation is valid;
- owner/lease/claim provenance proves which attempt was legitimately claimed;
- no newer owner/generation exists;
- no selector result/cursor evidence contradicts the repair;
- no submit intent exists;
- repair is monotonic and cannot reuse an attempt.

### Otherwise fail closed

For ambiguous/corrupt rows:

```text
zero selector calls
zero broker calls
zero cancel calls unless existing terminal/recovery policy explicitly owns it
structured invariant diagnostic
safe unresolved/terminal handling
```

Do not silently heal by choosing the largest counter.

---

## 12. Selector invocation idempotence

For each durable attempt, add/retain an explicit proof that selector work cannot be invoked concurrently more than once.

If current ownership CAS already provides this, test it rather than creating a second lock.

Required assertion pattern:

```text
attempt 1 selector calls <= 1 concurrent owner
attempt 2 selector calls <= 1 concurrent owner
...
```

A later bounded retry is a **new durable attempt**, not another callback execution of the same attempt.

This requirement aligns with PR #514's callback-liveness addendum but this PR owns only **counter/cursor authority**. Do not duplicate #514's deferred-cost sequencing implementation.

---

## 13. Broker safety invariants

This PR must not add or move broker authority.

### Absolutely required

- No direct Tradier submit/cancel added outside existing OSM authority.
- `broker_order_id` remains absent on unresolved retry/counter conflicts.
- `submitted_ts` remains null on unresolved retry/counter conflicts.
- `broker_ready` cannot become true from counter repair alone.
- Real OCC contract persistence remains required before broker-facing submission.
- Existing submit-intent / handoff proof remains required.
- Existing final risk checks remain required.
- Existing LIVE quote/confirmation gates remain required.

### Negative broker tests

Every malformed/conflict/race fixture must assert:

```text
selector calls = 0 where authority cannot be proven
broker submit calls = 0
broker cancel calls = 0 unless the existing lifecycle explicitly terminalizes/cancels that row
```

---

## 14. LIVE/PAPER and client identity invariants

Every retry claim, repair, schedule, cursor load, terminalization, and restart-recovery action must bind to exact identity.

Tests must include:

- same local-order-like metadata but different client -> cannot claim;
- same client but PAPER vs LIVE -> cannot claim;
- missing execution mode -> fail closed;
- malformed execution mode -> fail closed;
- whitespace/case normalization only where current canonical helpers explicitly permit it;
- stale cursor whose embedded identity belongs to another client/mode/order -> fail closed;
- recovery worker cannot “repair” missing durable identity using active runner context.

---

## 15. Diagnostics required

Every counter-authority conflict or repair decision should emit one structured diagnostic with enough state to reconstruct the decision without secrets.

Include, where available:

```text
event
client_id
execution_mode
local_order_id
signal_id
symbol
direction
lifecycle_state
materialization_status
materialization_generation
materialization_owner/current_owner
lease state
retry_attempt
breach_attempt_count
materialization_attempts
retry_attempt_in_flight
recovery_pre_claimed_attempt or equivalent durable claim proof
resolved/canonical attempt
resolution source
cursor present/version
next_retry_at
materialization_next_retry_at
broker_ready
broker_order_id present? (boolean or id if existing logging policy permits)
submitted_ts present?
reason_code
repair_performed boolean
```

Do not log Tradier auth, tokens, credentials, full account secrets, or unrelated PII.

Recommended stable events:

```text
SELECTOR_RECOVERY_ATTEMPT_AUTHORITY_CONFLICT
SELECTOR_RECOVERY_ATTEMPT_AUTHORITY_REPAIRED
SELECTOR_RECOVERY_ATTEMPT_CLAIMED
SELECTOR_RECOVERY_ATTEMPT_CLAIM_CAS_LOST
SELECTOR_RECOVERY_ATTEMPT_RESTART_RECOVERED
```

Names may differ if repository conventions dictate, but diagnostics must distinguish genuine corruption from a legitimate claim-in-flight state.

---

## 16. Required test matrix

### Resolver unit tests

1. all durable counters absent -> existing attempt-1 semantics preserved;
2. all durable counters 0 -> reviewed/reset semantics preserved;
3. all counters 1 -> resolves 1;
4. all counters 2 -> resolves 2;
5. durable `1/1/1`, valid preclaim 2 -> exact intended result;
6. preclaim behind durable -> fail closed;
7. malformed bool -> fail closed;
8. malformed float -> fail closed;
9. fractional string -> fail closed;
10. negative counter -> fail closed;
11. durable `2/1/1` without durable ownership proof -> fail closed;
12. durable `2/1/1` with the newly defined explicit durable claim state -> either safely normalize or resolve according to the reviewed design;
13. conflicting values with contradictory cursor/result evidence -> fail closed.

### OSM writer tests

14. schedule retry writes the canonical attempt authority atomically;
15. next-attempt claim cannot leave a normal durable split-counter state;
16. concurrent claim, exactly one succeeds;
17. stale generation claim fails;
18. stale owner claim fails;
19. broker-ready row cannot be reclaimed;
20. submit-intent row cannot be reclaimed;
21. direction-reversal reset clears old attempt/cursor authority only under valid CAS.

### Exact production regression

22. reproduce MCD `attempt1 -> DIRECT_QUOTE_ZERO_BID_ASK -> due retry -> attempt2 claim`;
23. assert no `2/1/1` unresolved state is emitted under normal execution;
24. assert attempt 2 carries required cursor/provenance;
25. assert selector recovery can proceed if all required evidence is valid;
26. if required cursor is absent, fail closed for the **correct cursor reason**, not because writer mirrors contradicted themselves.

### Crash-injection tests

27. crash after initial retry scheduling;
28. crash after next-attempt claim CAS;
29. crash before selector call;
30. crash during selector call boundary;
31. crash after selector returns retryable failure;
32. crash before retry schedule persistence;
33. crash after schedule persistence;
34. restart after each point produces deterministic one-owner behavior.

### Concurrency tests

35. watcher callback and retry worker collide;
36. two retry workers collide;
37. order monitor/restart recovery and retry worker collide;
38. stale process resumes after a newer generation owns row;
39. exactly one claim advances attempt;
40. no duplicate selector invocation for one durable attempt.

### Identity tests

41. LIVE vs PAPER separation;
42. client mismatch;
43. local_order mismatch;
44. signal/cursor identity mismatch;
45. missing durable mode;
46. malformed durable identity.

### Broker-negative tests

47. counter conflict -> zero broker submits;
48. cursor conflict -> zero broker submits;
49. stale generation -> zero broker submits;
50. duplicate claim loser -> zero broker submits from loser;
51. repair/convergence step alone -> zero broker submits.

### Regression tests

52. attempt-1 successful deferred selection unchanged;
53. ordinary non-deferred entry unchanged;
54. #504 request-scope structural terminal truth unchanged;
55. #512 direction-claim behavior untouched;
56. #514 actual-cost sequencing behavior untouched;
57. max retry ceiling unchanged;
58. entry cutoff unchanged;
59. selector thresholds unchanged;
60. sizing/risk limits unchanged;
61. exit engine/reconciler untouched.

---

## 17. Existing tests that must be reviewed/run

At minimum inspect and include relevant exact-head tests from:

```text
tests/test_p0_selector_recovery_cursor.py
tests/test_p0_selector_cursor_strictness.py
tests/test_p0_deferred_due_retry_ownership.py
tests/test_p0_fenced_retry_terminalization.py
tests/test_p0_seam4_e2e_deferred_lifecycle.py
tests/test_p0_deferred_breach_lifecycle_completion.py
tests/test_p0_deferred_materialization_bucket.py
tests/test_p0_pending_trigger_restart_recovery.py
tests/test_p0_amendment3_monotonic_generation_fencing.py
tests/test_p0_audit_blockers_401.py
tests/test_p0_direction_reversal_rearm.py
tests/test_p0_selector_recovery_deploy_preflight.py
```

Do not delete or weaken assertions merely because they expose a new conflict with the redesigned authority model. If an assertion is obsolete, explain exactly why the new invariant is stronger and replace it with an equivalent or stronger behavioral assertion.

Run the repository's exact-head P0 regression workflow before review-ready.

---

## 18. Forbidden implementations

This PR is **not accepted** if it does any of the following:

### Counter guessing

```python
attempt = max(retry_attempt, breach_attempt_count, materialization_attempts)
```

without durable proof.

### First-truthy guessing

```python
attempt = retry_attempt or breach_attempt_count or materialization_attempts
```

### Silent divergence tolerance

Changing the resolver to accept `2/1/1` simply because `retry_attempt` is labeled canonical.

### Blind repair write

Unconditionally rewriting all mirrors to the largest value on read.

### Reset-to-one workaround

Resetting a conflict to attempt 1, which can reuse selector/cursor work.

### Cursor bypass

Allowing attempt 2+ to run without the existing identity-bound cursor requirement.

### In-memory-only proof

Depending on `recovery_pre_claimed_attempt` or another Python variable that cannot survive process death as the only explanation for durable divergence.

### Retry-loop workaround

Adding sleeps, debounce, or logging suppression while leaving authority split.

### Broker dedup as substitute

Allowing multiple selector/submit paths and trusting broker-order dedup to save the system later.

### Scope creep

Do not change:

- per-position percentages;
- total exposure percentage;
- max positions;
- daily loss;
- selector moneyness;
- DTE;
- delta;
- spread;
- OI/volume;
- contract quality scoring;
- trigger geometry;
- momentum confirmation;
- broker limit pricing;
- exit policy.

---

## 19. Interaction with existing open PRs

### PR #514 — deferred real-contract cost before risk revalidation

#514 owns the `$173 placeholder > ~$172 budget` sequencing defect and confirmed-trigger callback liveness around that defect.

This PR must not duplicate #514's risk/selector ordering change.

Required compatibility:

- #514 may cause more legitimate confirmed breaches to reach deferred materialization;
- this PR must ensure those materialization retries have coherent durable attempt/cursor identity;
- if rebasing after #514, rerun exact production MCD retry tests plus #514's liveness tests.

### PR #512 — confirmed breach chooses CALL vs PUT

#512 owns opposite-direction co-arming and winner claim before broker handoff.

This PR must not alter directional arbitration.

Required compatibility:

- direction winner may enter deferred selector recovery;
- stale losing-direction worker must not claim retry attempt after loser cancellation/generation transition;
- exact identity/generation fencing must survive #512.

### PR #515 — capital policy

Capital-policy restoration/tuning is separate. Do not “solve” counter conflicts by changing budget values or retry counts.

---

## 20. Database/backfill policy

No production backfill is authorized by this spec.

Historical terminal rows, including the MCD EXPIRED row, are evidence and should remain untouched unless a separately reviewed migration is explicitly approved.

If deployment needs to handle currently nonterminal inconsistent rows:

1. first run a read-only preflight;
2. classify rows by exact reason;
3. auto-repair only rows satisfying the proven safe repair predicate in Section 11;
4. leave ambiguous rows fail closed;
5. log counts and local order IDs under existing privacy practices;
6. do not resurrect expired/canceled/rejected orders.

---

## 21. Required preflight changes

`ap/selector_recovery_deploy_preflight.py` must remain read only.

Update it only if required by the new authority contract.

It must detect:

- genuine unresolved counter conflicts;
- legacy/non-authoritative mirrors if the implementation formally demotes them;
- claim-in-flight durable state;
- attempt-2+ cursor validity;
- stale/expired owner lease;
- missing generation;
- LIVE/PAPER identity corruption.

A preflight must never mutate a row to make deployment pass.

If candidate nonterminal deferred-recovery rows exist at deploy time, preserve the current conservative deployment policy unless the PR explicitly proves a safer compatible rule.

---

## 22. Implementation deliverables

Codex must push code, not merely commentary.

Minimum deliverables:

1. writer/reader inventory in PR description or a companion design note;
2. canonical attempt-authority implementation;
3. fenced OSM claim/schedule changes required to make persistence coherent;
4. recovery/restart changes required for crash-safe interpretation;
5. resolver changes only if needed to represent a **proven durable state**, never to tolerate arbitrary disagreement;
6. selector cursor integration kept strict;
7. read-only preflight updated if authority model changes;
8. exact MCD production replay fixture/test;
9. crash/race tests;
10. LIVE/PAPER/client identity tests;
11. broker-zero negative tests;
12. exact-head P0 regression run.

---

## 23. Acceptance criteria

This PR may move from HARD HOLD only when all are true:

- The team can explain exactly how production reached `retry_attempt=2`, `breach_attempt_count=1`, `materialization_attempts=1`.
- Normal runtime can no longer persist that ambiguous split state.
- A valid next-attempt claim survives process crash without requiring transient Python context to explain it.
- Exactly one worker owns a durable selector attempt.
- Duplicate/stale workers cannot advance counters.
- Attempt counters never decrease except the existing separately fenced direction-reversal reset semantics.
- Attempt 2+ cursor strictness is preserved.
- Genuine unresolved disagreement still fails closed.
- No automatic repair guesses from `max()`, first-truthy, or missing evidence.
- No broker submit can occur from an unresolved counter/cursor state.
- `client_id`, `execution_mode`, local order identity, signal identity, generation, and owner fences are preserved.
- PAPER cannot claim LIVE and LIVE cannot claim PAPER.
- No risk/sizing/quality/trigger threshold changes.
- No direct broker submit/cancel path added.
- Existing #504 behavior remains intact.
- Compatible with #512 and #514 or rebased/tested after them.
- Exact-head CI/P0 regression is green.
- Actual diff is independently audited before merge.

---

## 24. Reviewer checklist

Before approving implementation, answer every item explicitly:

### Behavior

- Does this change LIVE behavior? **Yes, only the selector-recovery retry authority/liveness path.**
- Is it active when deployed? **Yes.**
- Does it intentionally make more trades? **No. It prevents legitimate retry state from self-corrupting.**

### Broker

- Any new direct broker submit path? **Must be no.**
- Any new direct broker cancel path? **Must be no.**
- Can counter repair set broker-ready? **Must be no.**
- Can unresolved conflict reach broker? **Must be no.**

### State mutation

- Does it mutate `orders.meta`? **Likely yes; audit exact CAS predicates.**
- Does it mutate positions? **Must be no.**
- Does it mutate proof_trades? **Must be no.**
- Does it mutate queue semantics? **Only if strictly necessary for current retry ownership; any change must be justified.**

### Identity

- `client_id` preserved?
- `execution_mode` preserved?
- local order identity preserved?
- generation monotonic/fenced?
- cursor identity bound?
- no PAPER/LIVE bleed?

### Production shape

- Exact MCD `2/1/1` fixture included?
- `DIRECT_QUOTE_ZERO_BID_ASK` retry path included?
- Restart after preclaim included?
- Concurrent claim included?
- Stale owner included?
- Missing cursor attempt-2+ included?

### Regression

- #504 structural request truth unchanged?
- #512 direction claim untouched?
- #514 deferred actual-cost sequencing untouched?
- selector thresholds unchanged?
- capital thresholds unchanged?
- exits untouched?

---

## 25. Merge recommendation now

**HARD HOLD.**

This branch contains the implementation contract only. Codex must implement the authority repair, tests, and production replay on this branch. After that, perform a full production audit of the actual diff before merge.

The objective is not to make the error disappear. The objective is to make it structurally impossible for normal runtime to create contradictory selector-attempt truth while preserving fail-closed behavior for genuinely unprovable state.
