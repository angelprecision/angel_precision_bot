# P0 SPEC — Durable `trade_queue` Lifecycle Convergence

**Status:** SPEC ONLY / DRAFT / HARD HOLD FOR IMPLEMENTATION

**Base audited:** `main@d404df34e00522ba2cce995b6a0129ba39b83944`

**Incident anchor:** 2026-09-09 Jason LIVE session showed a very large `trade_queue` WATCHING population relative to broker-confirmed fills. The exact fresh Supabase re-query was unavailable during this spec pass because the connector denied read permission, so implementation MUST re-query production and classify the exact rows before changing code. Historical audit evidence identified 74 Jason LIVE WATCHING rows and 2 broker-confirmed FILLED ENTRY orders for that session. Do not treat the 72-row difference as 72 missed trades. The defect is that the queue lacks a general durable convergence authority proving every nonterminal row eventually becomes either legitimately active or explicitly terminal.

---

## 1. Problem statement

`trade_queue.status='WATCHING'` is intentionally nonterminal on current main. `ap/queue.py` explicitly preserves WATCHING without `finished_ts` because the entry watcher owns trigger monitoring and later broker submission. That behavior is correct while ownership is genuinely active.

The production failure class is different:

> A queue row can remain `NEW`, `PROCESSING`, `WATCHING`, or `ARMED` after downstream lifecycle truth has already become terminal or otherwise authoritative.

Examples include:

- exact ENTRY order filled, but queue row never advances;
- exact ENTRY order canceled/expired/rejected, but queue row remains WATCHING;
- watcher geometry becomes terminal, but queue ownership remains nonterminal;
- retry budget exhausts, but queue lifecycle does not converge to the exact terminal reason;
- session ends with no breach and no outstanding legitimate retry/submit ownership, but queue row remains WATCHING indefinitely;
- manual/external close already has a narrow cleanup path in PR #518, proving this class exists, but that PR covers only broker-confirmed external/manual closes and is not a general queue convergence owner.

This creates three production risks:

1. **Missed-opportunity opacity** — operators cannot distinguish a valid never-breached setup from a stranded retry/materialization failure.
2. **Stale ownership** — a dead row may look behavior-active after the execution lifecycle is already over.
3. **False trade-flow metrics** — WATCHING count becomes a misleading proxy for active opportunities and makes it difficult to prove where trade flow is actually dying.

This PR is not about increasing trade count. It is about making every row explainable.

---

## 2. Canonical invariant

For every `trade_queue` row:

> `trade_queue` must converge to the most authoritative durable lifecycle truth for the exact client, execution mode, and signal lifecycle.

A row may remain nonterminal only while one of these is true:

- the watcher still legitimately owns an unbreached setup;
- a retry is durably scheduled and still eligible to run;
- an ENTRY submit is in a protected ambiguous/broker-unknown state that must fail closed;
- a downstream lifecycle transition is actively in flight and exact ownership can be proven.

Once none of those are true and exact downstream truth is terminal, queue state must become terminal exactly once.

### Stronger acceptance invariant

For a completed trading session, every queue row must classify into exactly one explainable lifecycle bucket:

- `NEVER_BREACHED_ACTIVE_OR_SESSION_EXPIRED`
- `TERMINAL_QUALITY_REJECTION`
- `TERMINAL_GEOMETRY`
- `RETRY_PENDING_ACTIVE`
- `RETRY_EXHAUSTED`
- `BROKER_SUBMIT_IN_FLIGHT_OR_AMBIGUOUS`
- `ENTRY_FILLED`
- `ENTRY_TERMINAL_NO_FILL`
- `DOWNSTREAM_POSITION_LIFECYCLE`
- `EXPLICIT_OPERATOR/MANUAL_CLOSE`

The production acceptance target is:

> `UNEXPLAINED_STALE = 0`

Do not define success as `WATCHING = 0` during market hours. Do not define success as “more fills.”

---

## 3. Non-goals / forbidden scope

This implementation must NOT:

- loosen selector gates;
- loosen spread, delta, OI, volume, premium, DTE, earnings, affordability, score, risk, or sector gates;
- alter entry confirmation logic;
- alter trigger geometry;
- alter watcher breach detection;
- alter premarket/live market-data authority;
- change contract selection;
- change sizing;
- change broker order submission;
- add any BUY or SELL path;
- add any broker cancel or replace path;
- change protective exit behavior;
- change position economics;
- create or rewrite proof rows except through already-existing downstream lifecycle owners;
- recreate PR #568;
- redesign #580 watcher ownership;
- redesign #603 due-retry scheduling;
- replace #579 position/EXIT convergence;
- replace #581 proof completion;
- create a second queue subsystem;
- terminalize based only on age/time elapsed;
- guess execution mode;
- guess signal identity from ticker/contract alone;
- infer LIVE/PAPER from environment, client name, broker URL, or account naming;
- use queue cleanup to manufacture broker truth.

If the exact authoritative evidence is ambiguous, the reconciler must fail closed and preserve the nonterminal owner with explicit diagnostics rather than inventing a terminal result.

---

## 4. Existing current-main seams to consume

Implementation begins by tracing current main, not by coding from this document.

At minimum inspect:

- `ap/queue.py`
  - `_TERMINAL_QUEUE_STATUSES`
  - `_mark_job()`
  - `_checked_watching_cas()`
  - `_checked_processing_error_cas()`
  - watcher deferral persistence
  - queue worker ownership semantics
- `ap_entry_watcher.py`
  - active watcher registry
  - terminalization reasons
  - trigger-ready transitions
  - retry ownership / cleanup
- `ap/order_state_machine.py`
  - ENTRY state taxonomy
  - broker evidence fields
  - PENDING_TRIGGER / RETRY_PENDING / FILLED / CANCELED / EXPIRED behavior
  - submit-intent protection
- `ap_recovery.py`
  - restart reattachment
  - recovered watcher ownership
  - stale trigger-ready logic
- `ap_overnight_reeval.py`
  - WATCHING -> reevaluation ownership
  - terminal geometry/rejection persistence
- `ap/operator_queue_read_model.py`
  - operator-visible state derivation
- any current position/reconciler seam used to prove that an ENTRY fill produced a canonical position
- PR #518 implementation if/when merged, because it already defines a narrow terminal queue mutation after external/manual close
- open PRs #580, #603, #579, #581, #597 to avoid duplicate or conflicting authority.

The implementation must reuse existing lifecycle truth. It must not become a second owner of watcher, retry, broker, position, or proof behavior.

---

## 5. Proposed architecture

Create one narrow reconciliation authority, ideally a dedicated module such as:

`ap/trade_queue_lifecycle_reconciler.py`

Name is not mandatory. Ownership is.

The reconciler should expose a pure/classification function and a guarded mutation wrapper.

Conceptual API:

```python
class QueueLifecycleDecision:
    action: str                 # KEEP_NONTERMINAL | TERMINALIZE
    target_status: str | None   # existing canonical queue status only
    reason_code: str
    authority_source: str
    evidence: dict


def classify_queue_lifecycle(*, queue_row, durable_state, now_et) -> QueueLifecycleDecision:
    ...


def reconcile_queue_row(*, queue_id, client_id, execution_mode, expected_status, now_et):
    ...
```

The classifier should be side-effect free where practical. Broker reads, DB reads, and CAS writes belong in wrappers/orchestrators.

### Why a dedicated reconciler is preferred

Queue lifecycle truth currently spans several independent owners. Baking additional cleanup conditionals into every writer increases the chance of asymmetric behavior and restart holes. A dedicated convergence layer can consume those durable facts without taking ownership away from them.

However, if current-main already contains an equivalent centralized lifecycle repair seam, extend that instead. Do not add duplicate infrastructure merely because this spec suggested a filename.

---

## 6. Exact evidence precedence

The reconciler must classify using the strongest available durable truth first.

### 6.1 Identity prerequisites

No mutation is allowed unless all required identity fields are proven:

- exact `trade_queue.id`;
- exact `client_id`;
- exact canonical `execution_mode` = `live` or `paper`;
- exact `signal_id` and/or canonical signal identity according to current-main lifecycle rules;
- any referenced order must belong to that exact client + mode + signal lifecycle;
- any referenced position must belong to that exact client + mode and be linked through canonical lifecycle identity, not ticker guessing.

Malformed, missing, or conflicting identity => classify as unresolved/fail-closed. Zero mutation.

### 6.2 ENTRY FILLED dominates queue nonterminal state

If an exact durable ENTRY order is broker-confirmed FILLED/PARTIAL_FILL according to current canonical semantics and belongs to this queue lifecycle:

- queue must not remain `NEW`, `PROCESSING`, `WATCHING`, or `ARMED` indefinitely;
- target queue status should use the repository's existing canonical terminal word, expected to be `FILLED` unless current-main semantics prove another existing status is correct;
- preserve exact local order ID, broker order ID, position ID if present, fill timestamp, and classification source in diagnostics;
- do not require proof row existence to terminalize the queue;
- do not create or repair a position in this PR.

If ENTRY claims FILLED but identity is malformed or contradictory, do not terminalize as FILLED. Surface the contradiction.

### 6.3 Terminal no-fill ENTRY order

If exact ENTRY lifecycle is durably terminal without a broker fill, e.g. canonical CANCELED / EXPIRED / terminal rejected state:

- queue should converge to an existing semantically correct terminal queue status;
- preserve the exact order terminal reason;
- if the order is CANCELED because broker truth is ambiguous/unresolved, do not simplify to a generic clean cancellation unless current order semantics prove broker exposure is impossible.

### 6.4 Terminal selector/risk/quality rejection

If exact durable lifecycle truth proves the setup reached a terminal market/quality/risk decision before broker submission:

- queue must converge to `REJECTED` or existing exact equivalent;
- persist the exact reason code, e.g. `SPREAD_TOO_WIDE`, `UNTRADEABLE_FOR_ACCOUNT_SIZE`, `NO_VALID_PLAYBOOK_DTE_CONTRACT`, etc.;
- do not retry terminal quality failures merely because retry ceiling changes elsewhere;
- do not downgrade a technical infrastructure failure into a market-quality rejection.

### 6.5 Terminal geometry

If watcher/reevaluation authority proves terminal geometry such as:

- `target_already_complete_terminal`
- `late_attachment_move_missed_terminal`
- explicit setup invalidation
- exact session-expired/no-breach lifecycle if current architecture defines it

then queue must converge to an existing terminal state with the exact reason preserved.

Do not infer geometry from current quote in this reconciler. Consume durable geometry authority from the owner that already made the decision.

### 6.6 Retry ownership remains nonterminal

If an exact retry owner exists and retry is still legitimately pending:

- preserve nonterminal queue state;
- do not terminalize because the row is old;
- include diagnostics such as retry owner, attempt count, next retry time, generation/attempt identity where available;
- #603 remains the scheduler/executor owner;
- this reconciler must never execute the retry itself.

### 6.7 Retry exhausted becomes terminal only from durable retry authority

If the authoritative retry owner has durably exhausted the configured retry budget:

- converge queue to the repository's existing terminal status (likely `ERROR` or `EXPIRED` depending current taxonomy);
- preserve exact terminal reason such as `BREACH_RETRY_EXHAUSTED:CURRENT_PRICE_FETCH_FAILED` or `RETRY_MAX_ATTEMPTS_EXCEEDED`;
- do not invent exhaustion from a local counter if durable ownership says otherwise.

### 6.8 Broker submit ambiguity is protected

If submit intent exists, broker POST outcome is ambiguous, broker reconciliation is pending, or broker truth cannot prove no exposure:

- NEVER terminalize simply to clean the queue;
- preserve the protected owner/nonterminal state;
- surface `BROKER_TRUTH_UNRESOLVED` or existing current-main diagnostic;
- no second ENTRY submit;
- no cancel authority added here.

The queue reconciler must fail in the safe direction even if that leaves an ugly row.

### 6.9 Session end / never breached

This is the most dangerous part and must be implemented only after tracing current watcher/session semantics.

A queue row may terminalize as session-expired/no-entry only when all are proven:

1. relevant trading session is over according to the repository's canonical market calendar;
2. watcher never durably breached or transitioned to trigger-ready;
3. no retry owner is active;
4. no submit intent exists;
5. no broker order ID exists;
6. no FILLED/PARTIAL ENTRY exists;
7. no canonical/open position exists;
8. no restart/reattach owner is active;
9. no later durable lifecycle generation supersedes the inspected one;
10. exact client + mode identity is proven.

Use the existing terminal status vocabulary if possible. Do not invent a new database enum/status unless schema requires it and current architecture cannot represent the truth.

If any one condition is uncertain, keep nonterminal and emit explicit unresolved diagnostics.

---

## 7. Mutation contract

All queue mutation must be atomic and guarded.

Preferred conceptual pattern:

```sql
UPDATE trade_queue
SET
    status = %s,
    finished_ts = NOW(),
    last_error = %s,
    result_json = <merged diagnostics>
WHERE id = %s
  AND client_id = %s
  AND status = %s
  AND <mode fence if mode is durable on row/payload>
RETURNING id;
```

The exact SQL must follow current schema and current execution-mode storage. Do not copy this literally if current main stores mode in payload or downstream tables.

### Required CAS behavior

- mutate only from the exact status observed by the classifier;
- if rowcount = 0, re-read and classify;
- if already terminal, treat as idempotent success only if terminal truth is compatible;
- if concurrently advanced to another nonterminal owner, preserve it;
- if concurrently advanced to conflicting terminal truth, surface conflict and do not overwrite;
- never blanket-update by ticker, contract, or signal alone;
- never update across client or mode.

### Idempotency

Running reconciliation repeatedly against an already-correct row must produce zero additional economic or lifecycle mutation.

---

## 8. Diagnostics contract

Every reconciliation mutation must leave enough evidence to answer:

- what was the prior queue status?
- what terminal status was chosen?
- why?
- which subsystem supplied authoritative truth?
- which exact order/position identity supported it?
- was broker exposure proven absent/present?
- was retry ownership active/exhausted?
- was the decision made during session, after session, or during restart?
- which execution mode was proven?

Preferred `result_json` additions, using names compatible with current repository conventions:

```json
{
  "queue_lifecycle_reconciled": true,
  "queue_lifecycle_previous_status": "WATCHING",
  "queue_lifecycle_target_status": "FILLED",
  "queue_lifecycle_reason": "ENTRY_BROKER_FILL_CONFIRMED",
  "queue_lifecycle_authority": "orders",
  "queue_lifecycle_execution_mode": "live",
  "queue_lifecycle_local_order_id": "...",
  "queue_lifecycle_broker_order_id": "...",
  "queue_lifecycle_position_id": "...",
  "queue_lifecycle_reconciled_at": "..."
}
```

Do not overwrite existing diagnostic keys blindly. Merge conservatively.

---

## 9. Trigger points

The reconciler needs both event-driven and restart/periodic coverage so crashes cannot strand rows.

### Event-driven

Call reconciliation after lifecycle events that are already terminal, but only if this does not duplicate existing queue mutation owners:

- ENTRY fill finalization;
- terminal ENTRY cancellation/expiry;
- terminal selector/quality rejection;
- watcher terminal geometry;
- retry exhaustion;
- canonical position lifecycle completion where queue still claims pre-entry ownership.

### Startup/restart

On startup, inspect only bounded, relevant nonterminal rows for the exact client/mode and recent trading sessions. Do not scan/replay arbitrary history.

### Periodic

A bounded reconciliation sweep may run periodically during/after session if current scheduler architecture has a safe home. It must be read-mostly and must not compete with active watcher/retry owners.

Do not introduce an aggressive loop that pounds Supabase every second. This is lifecycle convergence, not a substitute for event delivery.

---

## 10. Exact production replay requirement

Before implementation is considered mergeable, re-query the September 9 production rows and build a deterministic incident fixture.

For Jason LIVE on 2026-09-09, classify every queue row previously observed as WATCHING into one of:

1. never breached / legitimately watched;
2. terminal selector/risk/quality rejection;
3. terminal geometry;
4. retry pending at observation time;
5. retry exhausted;
6. submit/broker ambiguity protected;
7. ENTRY filled;
8. terminal no-fill ENTRY;
9. restart/ownership anomaly;
10. unexplained stale.

Required acceptance:

- every row accounted for;
- `unexplained stale = 0` after the proposed logic on the replay fixture;
- broker-confirmed fills remain exactly the broker-confirmed fill count; queue reconciliation must not create a trade;
- expected market/risk rejects remain rejects;
- active retry rows remain active;
- ambiguous broker-submit rows remain protected;
- no stale WATCHING survives when stronger terminal truth exists.

Also replay José PAPER and Tradefluence PAPER to prove mode isolation and to identify whether the same queue-convergence bug exists there.

---

## 11. Mandatory regression matrix

Use real PostgreSQL where the behavior depends on SQL/CAS/concurrency.

### A. Filled ENTRY

1. WATCHING + exact FILLED ENTRY + exact client/mode/signal -> queue FILLED.
2. ARMED + exact FILLED ENTRY -> FILLED.
3. PROCESSING + exact FILLED ENTRY after crash -> FILLED.
4. FILLED queue replay -> zero mutation.
5. FILLED ENTRY with wrong client -> zero mutation.
6. FILLED ENTRY with wrong mode -> zero mutation.
7. FILLED ENTRY with wrong signal/canonical identity -> zero mutation.
8. FILLED ENTRY with malformed broker ID if current canonical fill validator rejects it -> zero mutation / conflict diagnostic.

### B. Terminal no-fill orders

9. WATCHING + exact EXPIRED ENTRY -> appropriate terminal queue state.
10. WATCHING + exact CANCELED no broker exposure -> appropriate terminal queue state.
11. ambiguous submit/cancel state -> remains protected/nonterminal.
12. terminal order from PAPER cannot terminalize LIVE queue.

### C. Terminal quality/risk

13. SPREAD_TOO_WIDE -> REJECTED exact reason preserved.
14. UNTRADEABLE_FOR_ACCOUNT_SIZE -> REJECTED exact reason preserved.
15. NO_VALID_PLAYBOOK_DTE_CONTRACT -> REJECTED exact reason preserved.
16. risk/sector terminal block -> REJECTED exact current reason preserved.
17. transient quote/data failure must not be mislabeled as quality rejection.

### D. Geometry

18. target_already_complete_terminal -> terminal exact reason.
19. late_attachment_move_missed_terminal -> terminal exact reason.
20. invalidated setup -> terminal exact reason.
21. current quote alone cannot create geometry terminalization.

### E. Retry

22. RETRY_PENDING with future due time -> stays nonterminal.
23. RETRY_PENDING with due time passed but scheduler owner still valid -> stays nonterminal; #603 owns execution.
24. retry exhausted by durable authority -> terminal exact reason.
25. stale retry generation cannot terminalize newer generation.
26. retry attempt count cannot be reset by reconciliation.
27. new 20-attempt ceiling work remains independent; this PR does not change retry budget.

### F. Session end

28. never breached + session closed + no downstream ownership -> terminal session-expired/no-entry.
29. never breached but market still open -> remains WATCHING.
30. session closed but active retry owner -> remains nonterminal.
31. session closed but submit intent exists -> remains protected.
32. session closed but broker truth unavailable -> remains protected.
33. holiday/market-calendar uncertainty -> fail closed, no terminalization.

### G. Concurrency/CAS

34. watcher advances WATCHING -> TRIGGER_READY-equivalent concurrently while reconciler tries terminal session expiry -> reconciler CAS loses, zero overwrite.
35. order fill lands concurrently while reconciler classifies no-fill -> re-read and converge to FILLED.
36. two reconciler workers race -> exactly one mutation.
37. terminal queue row cannot be overwritten by stale reconciler.
38. concurrent manual-close cleanup (#518 if merged) and general reconciler -> compatible idempotent terminal truth, no regression.

### H. LIVE/PAPER isolation

39. exact same signal/contract in LIVE and PAPER -> no cross-mode evidence donation.
40. missing execution mode -> zero terminal mutation.
41. conflicting mode aliases -> zero terminal mutation.
42. client A evidence cannot mutate client B queue.

### I. Downstream invariants

43. queue reconciliation emits zero broker submit calls.
44. zero broker cancel calls.
45. zero broker replace calls.
46. zero new orders.
47. zero position creation/close mutation.
48. zero proof creation/mutation.
49. zero selector/risk behavior change.
50. zero watcher breach decision change.

---

## 12. Production file budget

Expected production scope should be small.

Preferred:

1. one dedicated reconciliation module OR one existing centralized lifecycle module;
2. minimal wiring in the safest existing scheduler/recovery/event hook;
3. `ap/queue.py` only if a guarded canonical helper must be added/reused;
4. tests;
5. P0 CI inventory if required.

HARD HOLD if implementation sprawls into selector, scanner, intelligence, sizing, broker transport, exit engine, proof logger, or broad position code without a separately proven necessity.

Do not touch `ap_master_control.py` for this issue.

---

## 13. Relationship to active PR stack

This work must not duplicate existing owners.

### #580 — recovered watcher lifecycle

#580 owns recovered watcher state/behavior restoration. This queue reconciler consumes its durable outcome; it does not replace watcher ownership.

### #603 — due retry runtime scheduler

#603 owns executing a due retry. This queue reconciler must preserve legitimate RETRY_PENDING state while #603 still owns it and may terminalize only when durable retry authority says exhausted/terminal.

### #579 — missing/canonical position + EXIT convergence

#579 owns position/EXIT convergence. Queue reconciliation must not create positions or repair EXIT economics. If an ENTRY is FILLED but position persistence is missing, queue can reflect the FILLED ENTRY truth while #579 separately repairs canonical position convergence, provided current semantics allow this without hiding the position defect. Diagnostics must retain the missing-position anomaly.

### #581 — proof completion

Queue completion must not depend on proof creation. Proof is downstream. #581 remains proof owner.

### #597 — terminal order vs behavior-active watcher

#597 owns removing behavior-active watcher ownership when durable terminal order already exists. This reconciler may consume that terminal order to converge queue state but must not duplicate watcher eviction logic.

### #518 — manual/external close downstream truth

If #518 is still open when implementation begins, compare its queue mutation carefully. Prefer one canonical terminal queue helper so manual-close cleanup and general convergence do not produce competing SQL semantics. Do not copy its logic wholesale if current main has moved.

### #568 — reverted

Do not restore it. The separate simple retry ceiling change is independent.

---

## 14. Observability / operator read model

After this work, operator diagnostics should distinguish:

- legitimately active WATCHING;
- retry pending;
- broker submit protected;
- terminalized no-entry;
- terminalized quality reject;
- filled;
- unresolved conflict.

If `ap/operator_queue_read_model.py` currently derives these states from orders/positions, prefer consuming that logic or sharing a pure classifier rather than inventing contradictory labels.

Do not make read-model changes mandatory unless current diagnostics cannot surface the new reconciliation reason.

---

## 15. Fail-first requirement

Before production code changes:

1. capture current-main behavior in focused regression tests;
2. prove at least one exact stale-WATCHING shape fails on unmodified base;
3. preserve the fail-first output;
4. apply the fix;
5. rerun unchanged test(s) green.

Minimum fail-first fixtures should include:

- WATCHING + exact FILLED ENTRY remaining WATCHING;
- WATCHING + exact terminal no-fill order remaining WATCHING;
- WATCHING after session close with no remaining owner, if current main truly reproduces that shape.

Do not invent a failing test for behavior current main already handles correctly.

---

## 16. CI / evidence gate

Before MERGE consideration, provide all of:

1. exact base SHA;
2. exact final head SHA;
3. full changed-file list;
4. `git diff --check` clean;
5. focused fail-first proof against base;
6. focused after-fix tests green;
7. real PostgreSQL CAS/concurrency matrix green;
8. exact September 9 Jason production replay green;
9. José/Tradefluence mode-isolation replay green;
10. exact-head full P0 green on unchanged head;
11. merge-ref full P0 green on unchanged head;
12. final cumulative diff audit;
13. confirmation of zero broker submit/cancel/replace changes;
14. confirmation of zero selector/risk/sizing changes;
15. confirmation of zero position/proof economic mutation changes;
16. confirmation that queue terminalization never relies on age alone;
17. confirmation `UNEXPLAINED_STALE = 0` in the replay classifier.

If exact-head and merge-ref run different canonical test inventories, HARD HOLD until corrected.

---

## 17. Rollback strategy

Rollback is code-only:

- revert the reconciliation wiring/module;
- do not attempt to blindly revert queue rows already terminalized from correct authoritative truth;
- any data rollback must be separately audited and must never turn broker-complete or terminal orders back into behavior-active WATCHING.

Because this changes persistent lifecycle status, implementation must be conservative enough that rollback is not expected to require data rewrites.

---

## 18. Merge decision rules

### MERGE only if

- current production rows were re-queried and classified;
- fail-first proves an actual current-main gap;
- implementation is narrow;
- exact identity/mode fences are enforced;
- legitimate active WATCHING remains active;
- retries remain owned by retry infrastructure;
- ambiguous broker state remains fail-closed;
- stronger terminal truth converges queue exactly once;
- all concurrency tests pass;
- full P0 exact-head and merge-ref pass;
- no broker/selector/risk/position/proof authority expanded.

### HOLD if

- implementation is directionally correct but production replay or exact-head evidence is incomplete;
- queue semantics are correct but one active PR dependency has not stabilized;
- diagnostics are insufficient to prove why rows terminalized.

### HARD HOLD if

- terminalization can occur from age alone;
- LIVE/PAPER identity can cross;
- ticker/contract guessing can mutate queue;
- broker ambiguity can be cleaned into a false terminal state;
- active retry ownership can be destroyed;
- filled ENTRY can be downgraded to rejected/expired;
- a stale reconciler can overwrite a newer lifecycle state;
- the PR changes broker submit/cancel/replace, selector, risk, sizing, exit, proof, or position economics;
- implementation duplicates #580/#603/#579/#581 ownership instead of consuming it;
- production replay leaves any unexplained stale rows without explicit fail-closed classification.

---

## 19. Codex work order

When this PR is ready for implementation, Codex should perform these steps in order:

1. Rebase the implementation branch onto then-current `main`.
2. Read this entire spec.
3. Read current `ap/queue.py`, watcher, OSM, recovery, overnight reevaluation, operator queue read model, and active PR diffs for #580/#603/#579/#581/#597/#518.
4. Re-query production for September 9 and reconstruct exact Jason/José/Tradefluence queue lifecycles.
5. Produce a row-classification table before editing code.
6. Identify which stale-WATCHING shapes are already fixed by current main or pending PRs.
7. Write fail-first tests only for the remaining uncovered current-main gap.
8. Choose the smallest existing central seam or add one dedicated reconciler module.
9. Implement pure classification first.
10. Add guarded CAS persistence.
11. Add event/restart/periodic wiring only where necessary.
12. Run focused tests.
13. Run real PostgreSQL concurrency matrix.
14. Run September 9 production-shaped replay.
15. Run exact-head full P0.
16. Run merge-ref full P0.
17. Audit final diff for accidental overlap with active PRs.
18. Report exact base/head, file scope, tests, replay counts, and unresolved dependencies.
19. Keep PR DRAFT / HARD HOLD until independent audit clears it.

Do not merge or deploy automatically.

---

## 20. Definition of done

The final system must make this statement true:

> For every Angel Precision opportunity, `trade_queue` can explain whether the setup is still legitimately alive, was rejected, became geometrically invalid, exhausted retry, entered broker submit protection, filled, or expired without entry. A row cannot remain WATCHING merely because no subsystem took responsibility for closing the bookkeeping loop.

That is the repair. It is lifecycle truth convergence, not trade-count inflation.