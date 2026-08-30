# P1 — Protect deferred RETRY_PENDING ownership from STUCK_TRIGGER_READY cleanup

## Status

**SPEC FIRST / HARD HOLD / IMPLEMENTATION REQUIRED / DO NOT MERGE OR DEPLOY FROM SPEC-ONLY STATE.**

Created: 2026-08-27

## Implementation-base writer resolution — 2026-08-30

The writer assumption captured at spec creation is historical. A full production
tree search on the rebased implementation base found no caller of
`ap.deferred_materializer.stamp_retry_pending()`; only its definition and
documentation references remain.

The active production `RUNNING -> RETRY_PENDING` handoff is:

```text
APExecutionCore
-> APOrderStateMachine.schedule_deferred_materialization_retry()
-> one fenced PostgreSQL JSONB merge
```

It atomically writes `RETRY_WAIT`, literal `broker_ready=false`, clears active
owner/lease fields, and persists matching `retry_attempt`,
`breach_attempt_count`, `materialization_attempts`,
`materialization_generation`, and `retry_max_attempts`. Those mirrors are
consumed by APRecovery and the OSM retry-claim CAS. A six-field legacy-only or
partially mirrored row is not executable retry authority and must fail closed.

The merge gate therefore requires the real OSM writer transition, including
`watcher_audit.reason_code=trigger_ready`, to survive classifier and durable
restart recovery without terminalization, owner replacement, attempt drift,
broker calls, or loss of restart-recovery diagnostics. CI also replays the same
fixture against the exact PR base SHA and proves the original
`STUCK_TRIGGER_READY -> terminal/cancel` action path before proving the fixed
head behavior.

This resolution supersedes references below that call the legacy helper the
current production writer; those sections remain as the original defect record.

Reference `main` at creation:

```text
d5a3288b55af95fbfa0dedaff89a867f04277461
```

Reference relationship:

```text
#526 merged into main at d5a3288b55af95fbfa0dedaff89a867f04277461
#521 remains a separate active-materialization protection PR at spec creation time
```

This PR is a surgical lifecycle-preservation follow-up to the exact issue explicitly deferred by the PR #521 r3 amendment.

It is **P1**, not P0, because:

- the defect can drop otherwise legitimate LIVE deferred entries;
- the shape is realistic during chain warmup, zero-quote recovery, or transient provider failures;
- current code can terminalize the row before its scheduled deferred retry;
- however this has not yet been proven as the primary cause of a specific zero-trade production incident;
- PR #521 owns the separately proven TMO `RUNNING` materializer race and must remain independently reviewable.

If implementation or production evidence proves this defect caused a specific LIVE trade loss at scale, severity may be raised. Do not raise severity merely because the code looks ugly. Humans have produced enough severity inflation already.

---

# 1. Production defect

`ap/deferred_materializer.py::stamp_retry_pending()` writes the canonical post-breach transient-failure shape:

```python
patch = {
    "materialization_status":          RETRY_PENDING,
    "broker_ready":                    False,
    "materialization_attempts":        int(attempt),
    "materialization_next_retry_at":   next_retry,
    "materialization_reason":          str(reason_code or ""),
    "materialization_last_failure_at": _now_utc().isoformat(),
}
```

It may additionally preserve a selector failure payload.

Importantly, this writer does **not** itself clear every field written by the prior materialization claim. Depending on the exact upstream writer path and merged ancestry, the durable row may therefore temporarily or persistently retain fields such as:

```text
materialization_in_flight   = true
materialization_owner       = <prior/current owner>
materialization_generation  = <generation>
materialization_lease_until = <possibly future lease>
lifecycle_state             = MATERIALIZING
```

while the canonical retry state is:

```text
materialization_status        = RETRY_PENDING
materialization_next_retry_at = <scheduled retry timestamp>
broker_ready                  = false
```

The stale-field question is a separate writer-shape concern that must be audited before changing any ownership fields. It is **not** permission to make this PR a deferred-materializer rewrite.

The immediately proven control-flow defect is classifier precedence.

Current `ap/pending_trigger_classifier.py` computes:

```text
watcher_reason
materialization_outcome
retry_status
retry_next_at
restart_rearm_status
restart_rearm_next_at
```

and then gives this ordering:

```python
# Priority 1
if watcher_reason == "trigger_ready":
    return STUCK_TRIGGER_READY

...

# Priority 6
if (
    retry_status in ("RETRY_PENDING", "RUNNING")
    or retry_next_at
    or restart_rearm_status == "RETRY_PENDING"
    or restart_rearm_next_at
):
    return WAITING_RETRYABLE
```

Therefore a canonical deferred retry row can never reach `WAITING_RETRYABLE` when `watcher_audit.reason_code == "trigger_ready"`.

That is the defect.

---

# 2. Exact vulnerable lifecycle

A normal deferred breach can legitimately look like:

```text
PENDING_TRIGGER
contract = DEFERRED:<TICKER>
watcher confirms breach
watcher_audit.reason_code = trigger_ready
materializer claims attempt
materialization_status = RUNNING
selector/chain work begins
```

Then a transient materialization failure occurs:

```text
chain warmup
zero usable quotes
provider timeout/blip
retryable selector-budget/data-unavailable outcome
other reason already classified by canonical retry policy as transient
```

`stamp_retry_pending()` records:

```text
materialization_status = RETRY_PENDING
broker_ready = false
materialization_attempts = N
materialization_next_retry_at = FUTURE
materialization_reason = <canonical retry reason>
materialization_last_failure_at = NOW
```

Before that retry is due, restart recovery / order-monitor recovery sees the still-`PENDING_TRIGGER` row.

Current classifier:

```text
watcher_reason = trigger_ready
-> Priority 1
-> STUCK_TRIGGER_READY
```

Current `PendingTriggerRestartRecovery` action table then does:

```text
STUCK_TRIGGER_READY
-> _terminalize_with_reason(
       reason="restart_stuck_trigger_ready_no_broker_proof"
   )
-> cancel_pending_entry(...)
-> durable terminal status
```

The deferred retry owner never gets its scheduled retry.

The setup can be lost even though the row contains canonical retry metadata proving that the previous materialization attempt explicitly chose **retry**, not terminalization.

---

# 3. Why this is distinct from PR #521

PR #521 owns the proven TMO race where pending-trigger recovery terminalized a row while a materializer was still actively `RUNNING`.

Its intended protected shape is approximately:

```text
lifecycle_state = MATERIALIZING
materialization_status = RUNNING
materialization_in_flight = true
materialization_owner = valid non-empty owner
materialization_generation = valid current generation
materialization_lease_until = future
no terminal/broker contradiction
```

and its protected classifier result is:

```text
MATERIALIZATION_IN_FLIGHT
```

This PR owns a different phase:

```text
materializer has stopped the current attempt
transient failure has been classified
canonical RETRY_PENDING has been durably stamped
next materializer attempt is scheduled
```

The two invariants are complementary:

```text
RUNNING current owner
-> #521 protects active attempt from cleanup

RETRY_PENDING canonical retry owner
-> THIS PR protects scheduled retry from cleanup
```

Do not merge these concepts into one loose "materialization-ish" predicate.

A `RUNNING` claim and a `RETRY_PENDING` backoff have different ownership truth and different required proof.

---

# 4. Relationship to PR #526

PR #526 merged at:

```text
d5a3288b55af95fbfa0dedaff89a867f04277461
```

It fixes deferred-plan ownership preflight and durable hydration behavior in `ap_execution_core.py`.

This PR does **not** own that seam.

#526 question:

```text
Does a plan that still needs materialization establish/prove ownership before selector/materialization work?
```

This PR question:

```text
After a materialization attempt truthfully schedules a retry, can pending-trigger cleanup destroy the row before the retry lifecycle resolves it?
```

Different phases. Different authority.

Do not modify `ap_execution_core.py` merely because #526 recently touched deferred lifecycle code.

---

# 5. Binding invariant

> A `PENDING_TRIGGER` deferred entry with a durably valid canonical materialization retry state must not be terminalized as `STUCK_TRIGGER_READY` merely because the watcher previously recorded `watcher_audit.reason_code=trigger_ready`.

For a valid not-yet-resolved materialization retry:

```text
watcher trigger evidence
+
canonical RETRY_PENDING metadata
+
no broker handoff
+
no terminal materialization contradiction
+
exact client/order/mode identity
```

must preserve the row under the existing deferred retry authority.

The pending-trigger recovery subsystem must not steal ownership from the deferred retry subsystem.

The reverse invariant is equally binding:

> Missing, malformed, contradictory, exhausted, terminal, cross-client, cross-mode, or broker-advanced retry state receives no accidental protection.

This is not "never clean up RETRY_PENDING."

This is "do not let a lower-information `trigger_ready` marker erase higher-information canonical retry ownership."

---

# 6. Preferred architecture

## 6.1 Reuse the existing retry path

Current restart recovery already has a canonical `WAITING_RETRYABLE` action:

```text
WAITING_RETRYABLE
-> _handle_retryable(...)
-> _retry_subtype(row)
-> MATERIALIZATION_RETRY
-> _verify_materialization_retry_ownership(...)
-> RETRY_OWNED on proven durable retry
```

This existing path is the preferred authority.

Do **not** create a new `MATERIALIZATION_RETRY_IN_FLIGHT`, `BACKOFF_OWNED`, `TRIGGERED_RETRY_HOLD`, or other duplicate taxonomy unless Codex proves the existing path cannot express the required invariant without ambiguity.

The preferred correction is a narrow classifier precedence / proof change that allows a truthful canonical materialization retry to reach the established `WAITING_RETRYABLE` / `RETRY_OWNED` path before `trigger_ready` can terminalize it.

## 6.2 Do not blindly move Priority 6 above Priority 1

A naïve change like:

```python
if retry_status == "RETRY_PENDING":
    return WAITING_RETRYABLE

if watcher_reason == "trigger_ready":
    return STUCK_TRIGGER_READY
```

is insufficient unless the retry shape is validated enough to avoid preserving malformed garbage.

The classifier is pure and has no DB reread authority. The recovery engine has stronger durable reread proof through `_verify_materialization_retry_ownership()`.

The implementation should therefore split responsibilities correctly:

```text
classifier:
    recognize a sufficiently coherent canonical retry candidate
    so trigger_ready does not immediately force terminalization

recovery action:
    reread durable row
    prove exact materialization retry ownership using existing verifier
    return RETRY_OWNED only on proof
    return UNRESOLVED/fail closed on malformed identity or retry data
```

Do not make the pure classifier pretend it has stronger proof than it does.

---

# 7. Mandatory pre-implementation inventory

Before changing production code, Codex must inventory all current writers and readers of:

```text
materialization_status
materialization_next_retry_at
materialization_attempts
materialization_reason
materialization_last_failure_at
materialization_outcome
broker_ready
materialization_in_flight
materialization_owner
materialization_generation
materialization_lease_until
lifecycle_state
```

At minimum inspect:

```text
ap/deferred_materializer.py
ap/pending_trigger_classifier.py
ap/pending_trigger_restart_recovery.py
ap/order_state_machine.py
ap_recovery.py
ap_entry_watcher.py
ap/order_monitor.py
ap_execution_core.py
```

and any other active production file returned by repository search.

For each writer/read path record:

```text
file
function
field(s) written/read
write atomicity if known
whether row is expected PENDING_TRIGGER
whether client_id/execution_mode/local_order_id are checked
whether broker submit/cancel authority exists
whether retry due-time is interpreted here or delegated
```

This inventory is required because stale active-owner fields may remain after `stamp_retry_pending()`. We must know whether they are harmless leftovers, required generation history, or incorrectly live-looking state before changing them.

---

# 8. Writer-shape audit: required, but scope-controlled

`stamp_retry_pending()` currently writes the canonical retry patch without explicitly clearing all prior active-claim fields.

Codex must answer:

1. Does the canonical OSM claim path already clear/replace active fields elsewhere atomically?
2. Can a real production row persist as:

```text
materialization_status = RETRY_PENDING
materialization_in_flight = true
materialization_owner = <non-empty>
materialization_generation = <positive>
materialization_lease_until = future
lifecycle_state = MATERIALIZING
```

3. Which production readers interpret those retained fields independently of `materialization_status`?
4. Is `materialization_generation` required for the subsequent retry claim / CAS?
5. Is `materialization_owner` historical evidence or active authority after RETRY_PENDING?
6. Should `lifecycle_state` remain MATERIALIZING during backoff or move to another existing canonical lifecycle state?
7. Is `materialization_in_flight=true` truthful during scheduled backoff, or should it be false?
8. Can clearing any of these fields create a duplicate materialization claim or defeat fencing introduced by #524/#526/#521?

### Scope rule

If the writer audit proves stale-field correction is necessary for correctness, Codex may add a **small, atomic writer normalization** in `ap/deferred_materializer.py` only if:

- the exact old and new durable shapes are documented;
- every affected reader is identified;
- the subsequent retry claimant is proven compatible;
- tests prove no generation/owner fence is lost;
- no broker/risk/selector behavior changes.

If writer normalization is not proven necessary, **do not touch the writer**. Fix classifier/recovery precedence only.

This PR is not permission to redesign the deferred materializer state machine.

---

# 9. Exact classifier requirement

The classifier must preserve these distinctions after #521 is accounted for:

```text
A. trigger_ready + valid active RUNNING materializer
   -> #521 MATERIALIZATION_IN_FLIGHT

B. trigger_ready + canonical RETRY_PENDING retry candidate
   -> WAITING_RETRYABLE (preferred)
   -> durable recovery proof decides RETRY_OWNED vs UNRESOLVED

C. trigger_ready + no active materializer + no valid retry candidate
   -> existing STUCK_TRIGGER_READY

D. terminal materialization outcome
   -> existing terminal classification

E. broker handoff / submitted evidence
   -> existing broker/reconciliation authority; not retry protection
```

Do not convert every `trigger_ready` row into retryable.

Do not protect a row solely because one of these is truthy:

```text
materialization_in_flight
materialization_owner
materialization_lease_until
materialization_next_retry_at
```

A single stale field is not ownership proof.

---

# 10. Canonical materialization retry candidate

Codex must use the **current writer contract**, not invent a second schema.

At current main, `stamp_retry_pending()` writes:

```text
materialization_status = RETRY_PENDING
broker_ready = false
materialization_attempts = integer attempt
materialization_next_retry_at = ISO timestamp
materialization_reason = non-empty reason code
materialization_last_failure_at = ISO timestamp
```

and `_verify_materialization_retry_ownership()` already requires, on durable reread:

```text
status = PENDING_TRIGGER
local_order_id exact
client_id exact
execution_mode exact
contract starts DEFERRED:
no broker_order_id
no submitted_ts
materialization_outcome absent or recognized retry-later outcome
materialization_status = RETRY_PENDING
broker_ready is exactly false
attempts within canonical max
next_retry_at parseable
reason non-empty
```

Prefer keeping this verifier the post-classification authority.

If #521 or later merged work has changed canonical durable mode resolution, generation fencing, or row shape, rebase first and use the merged canonical contract. Do not freeze this spec's snapshot over newer authoritative code.

---

# 11. Future vs due retry semantics

The original production-loss concern is specifically:

```text
RETRY_PENDING
materialization_next_retry_at = FUTURE
trigger_ready
```

This is unquestionably not `STUCK_TRIGGER_READY` because the system itself scheduled future work.

Codex must additionally trace the existing **due retry owner** and prove what happens when:

```text
now >= materialization_next_retry_at
```

The expected architectural rule is:

```text
pending-trigger recovery does not become the materializer
```

It should either:

- continue to recognize the canonical deferred retry owner while the actual deferred retry scheduler performs the due attempt; or
- follow an already-existing explicit due-retry handoff contract.

Do not add selector/materializer invocation to pending-trigger restart recovery merely because the retry is due.

Do not terminalize immediately at due time unless the existing canonical materializer policy explicitly proves exhausted/terminal state.

Required completion report must identify the exact due-retry executor and method.

---

# 12. Fail-first regression requirement

Before production code changes, add a deterministic regression against the unmodified implementation base.

Fixture must use a production-shaped deferred ENTRY row with at least:

```text
status = PENDING_TRIGGER
kind = ENTRY (if present in production shape)
client_id = exact non-empty client
execution_mode = live
local_order_id = exact non-empty id
signal_id = exact non-empty id
contract = DEFERRED:<TICKER>
broker_order_id = null/blank
submitted_ts = null/blank
watcher_audit.reason_code = trigger_ready
trigger_crossed_at = valid durable trigger evidence as required by current recovery fence
materialization_status = RETRY_PENDING
broker_ready = false
materialization_attempts = 1
materialization_next_retry_at = future timezone-aware ISO timestamp
materialization_reason = a real retryable reason from canonical policy
materialization_last_failure_at = valid timestamp
```

If current merged ancestry includes #521/#524 generation/owner fields, use the exact production writer shape and do not invent values merely to satisfy a test helper.

### Required fail-first behavior

On unmodified base:

```text
classify_pending_trigger_row(...)
-> STUCK_TRIGGER_READY
```

and through the real `PendingTriggerRestartRecovery` action path:

```text
-> _terminalize_with_reason(...restart_stuck_trigger_ready_no_broker_proof...)
-> terminal/cancel attempt
```

The test must fail because expected preservation does not occur.

Do not fake fail-first by directly calling a helper that production never reaches.

---

# 13. Required post-fix positive controls

## 13.1 Core future-backoff replay

Same fixture after implementation:

```text
trigger_ready
+ canonical RETRY_PENDING
+ future next_retry_at
-> classifier reaches WAITING_RETRYABLE (or exact documented equivalent)
-> restart recovery runs real retry ownership verifier
-> recovery outcome RETRY_OWNED
```

Required assertions:

```text
status remains PENDING_TRIGGER
contract remains DEFERRED:<TICKER>
client_id unchanged
execution_mode unchanged
local_order_id unchanged
signal_id unchanged
materialization_status remains RETRY_PENDING
materialization_attempts unchanged
materialization_next_retry_at unchanged
materialization_reason unchanged
materialization_last_failure_at unchanged
broker_ready remains false
broker_order_id remains empty
submitted_ts remains empty
```

And zero calls to:

```text
cancel_pending_entry
terminalization helper
watcher rearm
selector
materializer invocation from recovery
capacity check
quote-driven new entry work
broker submit
broker cancel
position mutation
proof_trades mutation
queue mutation
```

The recovery path may write existing restart-recovery diagnostics **only if current canonical behavior already does so**. Do not broaden mutation surface merely for observability.

## 13.2 Real transient reasons

Cover at least two real retryable materialization reasons drawn from canonical retry policy, ideally representing different families such as:

```text
data/quote unavailable
selector budget/provider temporary failure
```

Do not hard-code made-up reason strings if the canonical table exposes constants.

## 13.3 Due-time control

Prove behavior when:

```text
next_retry_at <= now
```

Expected result must match the existing deferred retry scheduler contract discovered during inventory.

Pending-trigger recovery must not terminalize merely because `trigger_ready` remains present.

## 13.4 Repeated recovery polls

Run restart recovery multiple times during the same future backoff.

Assert:

```text
no attempt increment
no next_retry_at drift
no duplicate retry schedule
no owner replacement
no generation replacement
no watcher rearm
no terminalization
```

Recovery is an observer of the retry owner, not another retry clock.

---

# 14. Mandatory negative-control matrix

At minimum cover all of the following.

## Retry shape validity

1. `trigger_ready` + no materialization retry metadata -> existing STUCK_TRIGGER_READY.
2. `materialization_status` missing -> no retry protection.
3. `materialization_status=RUNNING` -> #521 active-owner semantics, not this retry path.
4. `materialization_status=SELECTED` -> no retry protection.
5. `materialization_status=FAILED_TERMINAL` -> terminal authority wins.
6. `materialization_status=RETRY_PENDING` + missing `next_retry_at` -> no false retry ownership.
7. malformed `next_retry_at` -> fail closed.
8. timezone-naive `next_retry_at` -> use current canonical parser contract; if accepted today, document it and decide deliberately rather than silently changing parsing policy in this PR.
9. missing/zero/negative attempts -> verifier rejects.
10. attempts above canonical max -> verifier rejects / existing exhaustion authority wins.
11. missing materialization reason -> verifier rejects.
12. malformed/absent last-failure timestamp -> preserve exact current verifier semantics; if tightening it, justify separately and test.
13. `broker_ready=true` -> no retry protection.
14. broker order id present -> broker/reconciliation authority wins.
15. submitted timestamp present -> broker/reconciliation authority wins.

## Materialization outcome

16. recognized retry-later outcome + RETRY_PENDING -> eligible for canonical retry proof.
17. terminal materialization outcome -> terminal wins.
18. unknown non-empty materialization outcome -> no retry protection unless canonical policy explicitly recognizes it.
19. stale retry fields + terminal outcome -> terminal wins.

## Identity

20. missing `local_order_id` -> UNRESOLVED/fail closed.
21. wrong `local_order_id` on reread -> UNRESOLVED.
22. missing `client_id` -> UNRESOLVED.
23. wrong client -> UNRESOLVED; never protect cross-client row.
24. missing `execution_mode` -> UNRESOLVED.
25. wrong execution mode -> UNRESOLVED.
26. malformed execution mode -> fail closed.
27. LIVE runner observing PAPER row -> no protection/action.
28. PAPER runner observing LIVE row -> no protection/action.
29. signal identity contradiction where current recovery requires signal proof -> fail closed.

## Contract / row type

30. real OCC contract + stale RETRY_PENDING fields -> do not treat as deferred retry ownership unless current canonical verifier explicitly permits it.
31. blank contract shape -> use post-#526 canonical deferred semantics; do not regress blank deferred ownership.
32. non-ENTRY kind -> unchanged current behavior.
33. non-PENDING_TRIGGER status -> NOT_PENDING_TRIGGER / existing path.

## Watcher interaction

34. trigger_ready + valid canonical retry -> RETRY_OWNED, not STUCK.
35. invalidated terminal watcher reason + retry-looking garbage -> terminal invalidation wins according to canonical taxonomy.
36. orphan/no-watcher + valid canonical retry -> existing `_handle_orphan` retry ownership path remains valid.
37. healthy watcher-owned pre-breach row without retry -> unchanged.
38. restart-rearm retry fields must remain separate from materialization retry fields.

## #521 compatibility

39. valid active `RUNNING` materializer -> MATERIALIZATION_IN_FLIGHT once #521 is in ancestry.
40. RUNNING proof malformed -> no #521 protection.
41. RETRY_PENDING must not masquerade as #521 active RUNNING proof.
42. terminal outcome + stale `in_flight=true` -> terminal wins.
43. RUNNING -> RETRY_PENDING transition -> no recovery terminalization at any deterministic observation point represented by durable writes.

## #526 compatibility

44. blank deferred plan that acquired ownership via #526 can enter canonical retry and survive recovery.
45. real OCC + stale deferred metadata does not reopen deferred selection.
46. durable hydration reread failure behavior from #526 remains fail closed.

---

# 15. Deterministic transition test

Add at least one end-to-end-ish deterministic test around the actual state transition.

Preferred structure:

```text
1. construct/prove PENDING_TRIGGER deferred row
2. represent materialization attempt RUNNING using exact current writer shape
3. transient selector/materialization failure occurs
4. call the real stamp_retry_pending() or exact canonical writer path
5. pause before retry due time
6. run real PendingTriggerRestartRecovery
7. assert RETRY_OWNED / preserved
8. advance clock or build due-time fixture deterministically
9. run exact deferred retry executor
10. prove retry attempt, not pending-trigger cleanup, owns next action
```

No `sleep()` race tests.

Use injected clock/time helpers, monkeypatch, frozen timestamp fixture, Event/barrier, or explicit timestamps.

The test must prove the lifecycle, not merely a function's return string.

---

# 16. Changed-file budget

## Preferred production scope

After #521 is merged/rebased, prefer:

```text
ap/pending_trigger_classifier.py
```

Only touch:

```text
ap/pending_trigger_restart_recovery.py
```

if the existing retry action needs a narrow correction proven by tests.

Only touch:

```text
ap/deferred_materializer.py
```

if the mandatory writer-shape audit proves stale active-owner fields are themselves unsafe and a minimal atomic normalization is required.

## Preferred tests

Prefer extending existing authoritative files rather than creating an isolated toy harness:

```text
tests/test_p0_pending_trigger_restart_recovery.py
tests/test_p0_deferred_due_retry_ownership.py
```

Choose the smallest combination that can prove both classifier precedence and the real transition.

If a new focused test file materially improves clarity, name it something like:

```text
tests/test_p1_retry_pending_trigger_ready_fence.py
```

and register it in the authoritative workflow if repository convention requires it.

Do not spray the regression across six files because tests are free to write. They are not free to maintain.

---

# 17. Explicitly out of scope

Do not change:

```text
scanner generation
signal scores
score floors
entry trigger prices
CALL/PUT trigger semantics
late attachment continuation thresholds
watcher momentum poll requirements
stop activation semantics
contract moneyness
DTE
option delta
spread threshold
open interest threshold
volume threshold
premium limits
affordability
position sizing
capital percentages
max positions
daily loss controls
intelligence authority
selector quality policy
structural terminal taxonomy
Tradier order price/ladder logic
broker submit implementation
broker cancel implementation
fill monitoring
exit engine
positions mutation
proof_trades mutation
queue eligibility
LIVE/PAPER taxonomy
client routing
```

No schema migration is expected.

No new database table is expected.

No new retry worker is expected.

---

# 18. Forbidden fake fixes

Do not:

- add sleep/grace-period delays before cleanup;
- slow order-monitor polling;
- increase stale timeouts to hide the race;
- suppress `STUCK_TRIGGER_READY` logs while keeping terminalization;
- delete `trigger_ready` audit evidence;
- clear watcher audit just to avoid classification;
- turn all `trigger_ready` rows into `WAITING_RETRYABLE`;
- trust only `materialization_in_flight=true`;
- trust only a future `materialization_lease_until`;
- trust only `materialization_next_retry_at` without canonical retry state;
- default missing `execution_mode` from runner context;
- default missing `client_id` from runner context;
- infer LIVE from absence of PAPER;
- call selector from pending-trigger recovery;
- call deferred materializer from pending-trigger recovery unless an existing canonical due-retry contract already does so and the exact call path is proven;
- increment materialization attempts from restart recovery while merely observing backoff;
- rewrite `next_retry_at` on every recovery poll;
- replace owner/generation with `max()` or merge guesses;
- add broker submit/cancel authority;
- broaden eligibility to compensate for lost trades;
- loosen spread/OI/volume/moneyness/delta/DTE policy;
- combine #521 and this PR into an undifferentiated materialization state.

---

# 19. Client identity / execution mode requirements

Every production-shaped positive test must assert exact preservation of:

```text
client_id
execution_mode
local_order_id
signal_id where required
contract
materialization attempt count
materialization retry timestamp
```

Never use a fixture where LIVE/PAPER is omitted because "the test runner is live."

Missing mode is not LIVE.

Missing client is not current client.

A recovery system protecting the wrong client's row is worse than dropping one trade.

---

# 20. Broker and mutation audit

Before review-ready, Codex must grep/diff the final changed production files and explicitly report whether the PR adds or changes any path that can:

```text
submit broker order
cancel broker order
mutate order status
mutate positions
mutate proof_trades
write queue rows
change client_id
change execution_mode
change contract after materialization
change materialization attempt counter
change materialization retry time
change materialization owner/generation
```

Expected answer for the preservation path:

```text
broker submit = 0
broker cancel = 0
terminalization = 0
watcher rearm = 0
selector invocation = 0
position mutation = 0
proof_trades mutation = 0
queue mutation = 0
client/mode mutation = 0
```

If writer normalization is added, enumerate exactly which metadata keys change and why.

---

# 21. Diagnostics preservation

Do not solve this by erasing evidence.

The following diagnostics should remain available downstream where currently present:

```text
watcher_audit.reason_code = trigger_ready
materialization_reason
materialization_last_failure_at
materialization_next_retry_at
materialization_attempts
materialization_outcome when used
restart_recovery_cls / subtype diagnostics produced by current canonical recovery
```

The point is to interpret truthful metadata correctly, not to make the metadata less truthful.

No paper/live taxonomy pollution.

No new generic "retry" reason that collapses distinct selector/provider causes.

---

# 22. Required compatibility audits

## #521

Before review-ready, inspect the final #521 status.

If #521 merged:

```text
rebase this branch onto merged main
use exact merged MATERIALIZATION_IN_FLIGHT proof
prove RUNNING and RETRY_PENDING remain distinct
run #521 focused tests
```

If #521 is still open:

```text
do not copy its production implementation into this PR
record exact expected overlap/conflict
prove this PR can be rebased cleanly or document the minimal conflict
HOLD merge until ordering is explicitly decided
```

Recommended merge ordering if both are correct:

```text
#521 first
this PR second
```

because #521 owns the proven active `RUNNING` incident and establishes the final classifier shape this follow-up must respect.

## #526

Re-run #526 blank deferred ownership tests and verify:

```text
blank approved plan
-> ownership preflight
-> materialization
-> transient retry
-> pending-trigger recovery preserves retry
```

No regression to real OCC negative controls.

---

# 23. Required validation

At minimum run the final relevant existing suites after rebase:

```bash
python -m pytest -q \
  tests/test_p0_pending_trigger_restart_recovery.py \
  tests/test_p0_pending_trigger_lifecycle_integrity.py \
  tests/test_p0_deferred_due_retry_ownership.py \
  tests/test_p0_deferred_materialization_bucket.py \
  tests/test_p0_seam4_e2e_deferred_lifecycle.py \
  tests/test_p0_fenced_retry_terminalization.py \
  tests/test_p0_watcher_materialization_ownership.py
```

Also include the final #521 focused regression files after #521 is merged or explicitly tested for compatibility.

If this PR adds a new focused test file, include it.

Then:

```bash
python -m py_compile \
  ap/pending_trigger_classifier.py \
  ap/pending_trigger_restart_recovery.py \
  ap/deferred_materializer.py

git diff --check
```

If `ap/deferred_materializer.py` is untouched, py_compile may still include it because its writer contract is central to this PR.

Run the repository's authoritative exact-head workflow required for lifecycle P0/P1 entry changes.

Do not report CI from a prior SHA as evidence for final head.

---

# 24. Required completion report

Codex must post all of the following before requesting review:

```text
BASE SHA
HEAD SHA
whether #521 is merged/open at implementation finish
exact changed files
production files vs tests/docs

FAIL-FIRST
exact failing test name
unmodified classification result
unmodified recovery outcome
proof that cancel/terminal path was reached before fix

ROOT CAUSE
exact classifier precedence before
exact classifier precedence after
why trigger_ready is lower authority than canonical retry ownership

WRITER/READER INVENTORY
all materialization retry fields
all active owner/generation/lease fields
who writes them
who reads them
whether RUNNING -> RETRY_PENDING is atomic
whether stale active fields remain
whether writer normalization was required

RETRY AUTHORITY
exact due-retry executor file/function
future-backoff behavior
due-time behavior
exhaustion behavior

POSITIVE CONTROLS
future RETRY_PENDING + trigger_ready result
repeated recovery polls
real transient reasons
RUNNING #521 compatibility
#526 blank-deferred compatibility

NEGATIVE CONTROLS
malformed timestamp
missing reason
attempt bounds
terminal outcome
broker-ready/broker-submit evidence
real OCC stale metadata
cross-client
cross-mode
missing identity
LIVE/PAPER isolation

MUTATION AUDIT
terminalize calls
rearm calls
selector calls
materializer calls from recovery
broker submits
broker cancels
order status writes
positions writes
proof_trades writes
queue writes
retry attempt/time writes during observation
owner/generation writes during observation

DIAGNOSTICS
proof watcher_audit.reason_code remains preserved
proof materialization reason/timestamps remain preserved

TESTS
focused tests
adjacent tests
#521 compatibility tests
#526 compatibility tests
exact-head CI
py_compile
git diff --check

git diff main...HEAD --stat

FINAL RECOMMENDATION
MERGE / HOLD / HARD HOLD
```

---

# 25. Review questions

Independent reviewer must answer explicitly:

1. Does this change live behavior?
2. Is it flag-off or active by default?
3. Does it touch broker submit/cancel?
4. Does it mutate orders, positions, proof_trades, or queue?
5. Does it preserve exact `client_id` and `execution_mode`?
6. Does it use the real production `stamp_retry_pending()` metadata shape?
7. Does it preserve diagnostic reason codes downstream?
8. Could it pollute PAPER/LIVE taxonomy?
9. Can a stale fake RETRY_PENDING row now survive indefinitely?
10. Can a valid retry still be terminalized by `trigger_ready`?
11. Does terminal materialization still beat retry-looking stale fields?
12. Does broker handoff evidence still beat pending-trigger retry preservation?
13. Does #521's active RUNNING fence still work?
14. Does #526's deferred ownership preflight still work?
15. Does recovery merely observe the retry owner, or did it accidentally become another materializer/retry scheduler?

---

# 26. Merge gate

**HARD HOLD** until implementation exists and all of the following are proven:

```text
fail-first reproduced on implementation base
canonical RETRY_PENDING + trigger_ready is preserved
real PendingTriggerRestartRecovery path returns RETRY_OWNED
no terminalization during valid backoff
no rearm during valid backoff
no selector/materializer invocation from recovery observation path
no broker submit/cancel added
exact client/mode identity preserved
terminal/broker contradictions still fail closed
RUNNING remains owned by #521 semantics
blank deferred ownership remains compatible with #526
writer stale-field question explicitly resolved
future and due retry behavior both proven
focused + adjacent tests green
exact-head CI green
independent diff audit complete
```

Until then:

```text
HARD HOLD
```

Expected final lifecycle:

```text
confirmed trigger
-> deferred materializer owns attempt
-> transient materialization failure
-> canonical RETRY_PENDING stamped
-> pending-trigger recovery observes retry ownership and stays out
-> deferred retry scheduler owns the next attempt
-> selector eventually succeeds OR canonical exhaustion/terminal policy resolves
-> broker handoff proceeds only through existing authority
```

Never again:

```text
confirmed trigger
-> materializer says RETRY LATER
-> recovery sees old trigger_ready marker
-> recovery says STUCK
-> local order canceled before retry
```

That is the entire purpose of this PR. Keep it that small.
