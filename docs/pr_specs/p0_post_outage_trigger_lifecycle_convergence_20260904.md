# P0 — Converge trigger-ready watcher lifecycle after DB/readiness outage

## STATUS

**AMENDED IN PLACE — HARD HOLD. DO NOT MERGE OR DEPLOY.**

Base: `84d8d61278d45040d8a6830e58c0ac912d683ff2`
Current implementation/code head: `4a57c63bfa354b1c5dc7f8647f44c47b4ebd668c`
PR state: **Draft / open / HARD HOLD**. The final branch head including this
documentation attestation is recorded in the PR body; this spec commit is
documentation-only and does not alter production code.
Audited deployed main: `98eeaadae05f9e4e1db624703ec1e3cd758b732c`
Original head before amendment: `dd41efaac6db81e3762e87a05c8c90276019fe08`
Prior amended head before live-base rebase: `bb7d2084f014ff216a7915873e34f5bbd92bf4be`
Amendment applied: 2026-09-09

Dependencies (§STATUS binding order):
- PR #568 (deferred selector/materialization retry authority): **merged** into
  current main at `09d30cff2f418e81c1a9bcec734f14f9f82a9a93`
- PR #569 (fresh market truth / late watcher recovery): **open/draft** and
  remains a separate ownership boundary; it is not included in this amendment

Implementation on this branch is preparatory only, per explicit override.
Before merge:
1. #569 remains separately finalized and audited; no #569 code is copied here.
2. This branch must remain based on the actual committed `main` SHA above.
3. The PEP and September 8 Jason LIVE fail-first replays in
   `tests/test_p0_post_outage_trigger_lifecycle_convergence.py` must pass
   against the exact amended head.
4. Full P0 regression must pass at the exact amended HEAD SHA.

## AMEND PR #580 IN PLACE — Corrections applied 2026-09-09

The September 8/9 amendment applies the remaining recovery-admission and
replacement-convergence corrections surgically. Scope boundary unchanged:
PR #580 owns only recovered durable `PENDING_TRIGGER` watcher admission and
its lifecycle/registry convergence.

### Current amendment closure

- Complete plan-to-row identity proof now covers local order, signal and
  canonical signal IDs, exact client/mode, ticker, side, generation
  authorities, watcher owner/token, and trigger generation/cursor authorities.
  Missing, malformed, blank, conflicting, or whitespace-normalized identity
  is HOLD. Recovery does not fabricate UUIDs, default side, or fall back to
  `self.mode`.
- Every generation authority (durable column, metadata/meta, and candidate)
  is parsed independently as a strict positive integer. Zero, negative,
  boolean, fractional, malformed, blank, row-only, candidate-only, and
  conflicting values fail closed.
- Production recovery requires exact OSM client identity, local order
  identity, PostgreSQL connection, `FOR UPDATE` row lock, and an exact row.
  Missing/non-string/blank identity or any lock/read failure is HOLD. The
  explicit `_test_only_allow_recovery_without_row_lock` seam is test-only.
- The lock remains held through final durable proof, canonical broker/
  materializer handoff proof, lifecycle restoration, candidate registry/dedup
  and per-call provenance commit. Candidate rollback is failure-atomic; an
  unrepaired lifecycle transition is an explicit quarantine that blocks a
  duplicate recovery attempt.
- A replacement candidate becomes durable watcher owner first. The exact
  displaced incumbent is then identity-reread, cancelled through the existing
  `cancel_pending_entry` authority, reread as terminal, and only then removed
  from `_pending` and dedup. Cancellation failure/exception/identity drift
  preserves the incumbent byte-for-byte and returns HOLD.
- A post-lock broker/materializer owner is reread before trigger dispatch;
  the recovered callback is suppressed and no second economic attempt or
  invented broker cancel is made.
- `materialization_resume=True` remains on the existing #596 adoption-CAS
  path and never enters the #580 transaction or lifecycle bridge.

### Correction 1 — Canonical broker handoff must block recovery before watcher admission

`_restore_recovered_watcher_lifecycle()` now adapts the existing canonical
`ap.pending_trigger_classifier` authorities before any lifecycle write:
`classify_pending_trigger_row()` owns populated row-level
`broker_order_id`/`submitted_ts`, `has_broker_handoff_evidence()` owns durable
submit-intent markers, and `is_active_materialization_in_flight()` owns the
complete current materializer proof. Any canonical evidence → HOLD; an
unavailable canonical authority also → HOLD. The bridge no longer maintains a
second broker-handoff vocabulary.

`watcher_audit.reason_code == "trigger_ready"` is deliberately not broker
evidence. It proves watcher breach confirmation only. At this lifecycle-bridge
boundary, a PENDING_TRIGGER row with that audit label but no canonical
broker/materialization handoff evidence remains eligible for restoration. The
shared due-retry classifier precedence remains untouched for its separate P0
owner (#596).

### Correction 2 — Recovery identity must fail closed

`_restore_recovered_watcher_lifecycle()` now validates the full durable
identity from column values only — no metadata fallback for blank columns,
no UUID fabrication, no mode fallback, no default side. Validated:
`signal_id`, `ticker`, `client_id`, `execution_mode` (exactly `"live"` or
`"paper"`, case-sensitive, no normalization), `local_order_id`,
`canonical_signal_id`, and `side` (exactly `"CALL"` or `"PUT"`). Metadata
aliases are parsed independently; malformed or conflicting aliases → HOLD.

Materialization generation is parsed independently from every populated
signal/metadata authority. Each populated value must be a strict positive
Python integer, and multiple populated values must agree exactly. Explicit
zero, negative, boolean, string, float, malformed, or blank values never fall
through to another source. A genuinely absent source may fall back to one
valid source under the existing optional-generation contract.

### Correction 6 — Recheck the exact durable row before recovery classification

`watch(recovery_rearm=True)` now reloads the exact durable row and requires
`status=PENDING_TRIGGER` plus one coherent
`local_order_id`/`signal_id`/`canonical_signal_id`/`client_id`/
`execution_mode` identity across the incoming plan, row columns, and row
metadata. Conflicting or mismatched aliases, unavailable rows, and durable
broker-handoff evidence HOLD before the shared classifier or watcher
registration. This closes the plan-to-row identity gap without changing
ordinary admissions or retry/terminal classifier precedence.

### Correction 3 — Lifecycle import failure must fail closed

`_EW_LIFECYCLE_OK == False` path changed from soft success
`(True, "recovery_lifecycle_module_unavailable_soft_ok")` to HOLD
`(False, "recovery_lifecycle_unavailable_hold")`. A bridge that cannot
read lifecycle state cannot determine whether admission is safe.

### Correction 4 — Lifecycle restoration is now atomic (validate before register)

The recovery lifecycle validation block in `add_signal()` now runs
**before** `_pending.append()` and `_dedup_set.add()`. Preferred order:
1. validate identity, 2. validate broker handoff, 3. restore lifecycle,
4. register watcher. A HOLD now leaves the registry completely clean with
no rollback needed.

### Correction 5 — Retry ownership preserved

No changes made to `#568`/`#569` selector or materializer retry counters.
`WATCHING → TRIGGER_READY` remains legal per existing `LEGAL_TRANSITIONS`.
Recovery never creates a second broker submission attempt.

## Implementation summary (current amendment)

Production files changed:
- `ap_entry_watcher.py` — 430 insertions / 113 deletions. Changed recovery
  identity/generation/final-authority helpers, row-lock fence, lifecycle
  repair, candidate commit/replacement convergence, strict recovery `watch()`
  construction, and `add_signal()`'s recovery-only constructor guard.
- `ap_entry_watcher/__init__.py` — 36 insertions. Changed the package poll
  dispatch seam to reread durable ownership after the admission lock.

Test/CI files changed:
- `.github/workflows/p0_regression.yml` — 1 insertion / 1 deletion; exact-head
  and merge-ref jobs run the complete lifecycle-integrity file plus the
  canonical P0 inventory with PostgreSQL enabled.
- `tests/test_p0_pending_trigger_lifecycle_integrity.py` — 542 insertions /
  17 deletions; complete identity/generation/OSM-fence/lifecycle failure,
  replacement convergence, materialization-resume isolation, and real
  PostgreSQL lock-race coverage.
- `tests/test_p0_post_outage_trigger_lifecycle_convergence.py` — 4 insertions;
  explicit test-only fence seam for the September 8 Jason LIVE replay.
- `tests/test_p0_reattach_no_cancel_on_missing_quote.py` — 5 insertions;
  explicit test-only fence seam and complete identity fixture.
- `tests/test_p0_prebreach_stop_activation.py` — 3 insertions; existing
  production-shaped recovery fixtures explicitly opt into the test-only
  no-PostgreSQL seam.
- `tests/test_p0_seam4_e2e_deferred_lifecycle.py` — 34 insertions; preserves
  the #596 materialization-resume argument and adoption-CAS in the harness.
- `tests/test_p0_watcher_conflict_cancellation_proof.py` — 1 insertion;
  explicit test-only fence seam.

`ap/pending_trigger_restart_recovery.py` and all #596 production retry
authority remain unchanged by this amendment.

Files NOT touched (§4/§5 binding):
- `ap_lifecycle.py`, `ap/order_monitor.py`, `ap/preopen_readiness.py`,
  `ap_overnight_reeval.py`, `ap/order_state_machine.py`,
  `ap/selector_retry_policy.py`, `ap_execution_core.py`,
  `ap/exposure_gate.py`, contract selector, scanner, sizing, scoring,
  exit engine, reconciler, broker adapters, proof_trades, queue,
  schema. Zero new broker submit/cancel authority. No durable retry
  counter added.

Local final status: focused amendment class `83 passed, 1 skipped`; complete
`tests/test_p0_pending_trigger_lifecycle_integrity.py` `126 passed, 1
skipped`; combined lifecycle/recovery set `514 passed, 1 skipped`.
The local PostgreSQL lock-race test is skipped when
`INTELLIGENCE_POSTGRES_TEST_URL` is absent; CI is the required PostgreSQL
evidence. No local PostgreSQL server was available (`pg_isready` returned
`localhost:5432 - no response`).

The final exact-head and merge-ref CI run after the documentation attestation
is `34358977197` on the final pushed branch head recorded in the PR body. Both
jobs passed the complete 5,285-test inventory with PostgreSQL enabled:
`p0-tests` job `102490772911` and `p0-merge-ref-tests` job `102490773249` each
reported `5285 passed, 2 warnings`. The exact-head log records the real
row-lock race, same/opposite durable replacement, cancellation-failure
preservation, missing-OSM-client HOLD, materialization-resume isolation, and
post-lock owner suppression tests as passed. The preceding code/test parent
run was `34358392622` at `4a57c63bfa354b1c5dc7f8647f44c47b4ebd668c`.

This PR owns one failure class:

> A valid deferred entry that regains watcher ownership after a database/readiness interruption must restore one coherent lifecycle authority and progress through the existing callback path. It must not loop forever on `NONE -> TRIGGER_READY`, reset callback attempt state, or terminalize a valid retry merely because restart reconstruction omitted lifecycle state.

No strategy thresholds or broker semantics belong here.

This PR does not modify `ap.pending_trigger_classifier` retry precedence or
`_resolve_trigger_callback_disposition()` terminal-reason aliases. Those are
the separate due-materialization-retry-liveness (#596) and terminal-watcher-
convergence (#597) ownership seams.

---

## September 4 production defect — PEP

Signal:

```text
signal_id: da9db343-8cec-46af-abd0-e43e73bfa4c6
ticker: PEP
execution context: Jason LIVE
```

Observed repeatedly after the morning Supabase/readiness incident:

```text
[LIFECYCLE] ILLEGAL_TRANSITION
PEP NONE -> TRIGGER_READY
owner=WATCHER
reason=trigger_breached_entry_submitted

WATCHER_TRIGGER_CALLBACK_ATTEMPT
PEP attempt=1/3 kept_in_pending=true

WATCHER_TRIGGER_OWNERSHIP_RETAINED
PEP disposition=KEEP_WATCHER
```

The loop repeated for minutes.

The signal never reached a real contract selection / broker-ready handoff and produced no broker POST.

It later terminalized through restart cleanup as:

```text
restart_stuck_trigger_ready_no_broker_proof
```

This is not proof that PEP was bad. It is proof that the reconstructed watcher/lifecycle authorities disagreed.

Current main itself documents the invariant in an existing test comment: `NONE -> TRIGGER_READY` is illegal because `LEGAL_TRANSITIONS` requires the signal to be `WATCHING` first, and bypassing the normal `add_signal()` lifecycle registration can prevent submit.

The production restart/reattach path reproduced that class of failure.

---

## Relationship to pending PRs

### #569 — fresh market truth / late watcher recovery

#569 owns the late/ownerless overnight recovery policy and the freshness/deadline boundary around watcher installation. It is relevant upstream.

It does **not** own the entire September 4 PEP failure because the current PR changes restart recovery/readiness/reeval consumers but does not establish one canonical callback-attempt/lifecycle restoration invariant inside the entry watcher/lifecycle seam.

Do not copy #569's market-truth logic here.

After #569 is finalized, this PR must rebase and prove the same resolved lifecycle behavior with #569's recovery output.

### #568 — validity-bound deferred materialization retry

#568 owns deferred selector/materialization attempt authority. Its attempt counter is not automatically the same authority as `APEntryWatcher`'s `_trigger_attempts`.

Do not merge the two counters casually.

This PR must first document exactly which counter owns:

- watcher callback retry;
- deferred selector/materializer retry;
- restart recovery attempt;
- terminalization.

Only consolidate authorities if the actual caller trace proves they represent the same economic attempt.

### #560 / older readiness PRs

Older readiness PRs are stale implementation vehicles. Use them as historical evidence only. Do not cherry-pick broad old diffs.

---

## Required production code trace before implementation

Trace the exact path for a preclaimed/restarted deferred signal:

```text
startup / preopen readiness
-> pending-trigger restart recovery
-> durable order/meta read
-> watcher reconstruction / watch()
-> lifecycle state restoration
-> quote poll / breach confirmation
-> lifecycle TRIGGER_READY transition
-> on_trigger callback
-> execution core disposition
-> watcher retry ownership
-> next callback or terminalization
```

For every changed function record:

```text
caller
-> state read
-> identity/generation authority
-> lifecycle mutation
-> retry mutation
-> return classification
-> downstream consumer
```

Do not accept a lifecycle helper in isolation while one restart caller still bypasses it.

---

## Binding authorities

The implementation must explicitly resolve these independent authorities.

### Identity

- `client_id`;
- `execution_mode`;
- canonical `signal_id`;
- local order ID;
- direction/side;
- OCC only after materialization;
- lifecycle/materialization generation;
- watcher ownership token or durable equivalent.

### Lifecycle

One canonical durable source must determine whether the reconstructed signal is:

- WATCHING;
- TRIGGER_READY;
- broker-ready/submitting;
- terminal;
- invalid for recovery.

`NONE` is not an acceptable silent default when durable order/meta proves an owned watching/retry lifecycle.

### Retry

The implementation must distinguish and prove the authority for:

1. watcher callback attempt;
2. selector/materialization attempt;
3. restart recovery attempt;
4. broker submit attempt.

A log that emits `attempt=1/3` forever is not bounded retry semantics.

Do not increment a money-path attempt merely because an infrastructure/lifecycle write failed before selector or broker work occurred unless the binding authority explicitly defines that as an attempt.

---

## Required state behavior

### Healthy first attempt

```text
durable WATCHING
-> watcher owns
-> breach confirmed
-> TRIGGER_READY legal transition
-> callback attempt 1
-> execution core progresses
```

Exactly one callback per accepted retry window.

### Retryable callback result

```text
TRIGGER_READY / owned retry
-> callback returns KEEP_WATCHER or RETRY_WAIT
-> durable retry authority preserved
-> lifecycle remains a legal resumable state
-> next retry occurs no earlier than next_retry_at
-> attempt/counter semantics advance exactly as defined
```

No busy loop.

### Restart with durable WATCHING

```text
process dies
-> restart reads exact durable owner/state
-> reconstructs watcher
-> restores lifecycle WATCHING before any TRIGGER_READY emission
-> future breach follows normal path
```

### Restart with durable TRIGGER_READY retry

```text
process dies after trigger claim but before successful callback completion
-> restart does NOT synthesize NONE
-> exact same signal/order/generation resumes
-> no duplicate selector/broker submission
-> retry owner remains durable
```

### Broker-ready or submit-intent state

Once exact durable broker-ready/submit ownership exists:

- watcher must not re-fire the breach callback as a new attempt;
- restart must hand off to the canonical broker-ready/OSM recovery consumer;
- zero duplicate submit authority.

### Ownership loss / stale generation

If exact owner or generation is stale/conflicting:

- no lifecycle mutation to TRIGGER_READY;
- no selector;
- no broker call;
- explicit HOLD/recovery classification;
- diagnostics preserve the conflicting fields.

---

## PEP exact positive control

Build a behavioral replay using the September 4 PEP shape:

```text
signal da9db343-8cec-46af-abd0-e43e73bfa4c6
LIVE
valid deferred order
watcher reconstructed after DB/readiness interruption
lifecycle registry starts without in-memory state
```

Fail-first should reproduce current behavior:

```text
NONE -> TRIGGER_READY
ILLEGAL_TRANSITION
KEEP_WATCHER
attempt 1/3 repeats
```

After the fix, the same durable data must produce one of only two valid outcomes:

1. exact durable recovery restores a legal WATCHING/TRIGGER_READY retry state and progresses to the canonical execution callback; or
2. authority is genuinely unprovable, so the path HOLDs without broker mutation and without falsely terminalizing as a legitimate trade rejection.

The test must not hard-code that PEP necessarily deserves a fill. Contract quality / final risk gates remain downstream and may legitimately reject it.

---

## Failure timing matrix

Execute process-death / DB-failure behavioral tests at:

1. before watcher reconstruction;
2. after watcher object creation but before durable owner registration;
3. after owner registration but before lifecycle WATCHING restore;
4. after WATCHING restore but before breach poll;
5. after breach evidence but before TRIGGER_READY persistence;
6. after TRIGGER_READY persistence but before callback;
7. during callback before selector;
8. after selector starts but before materialization persistence;
9. after materialization persistence but before broker-ready copyback;
10. after broker-ready copyback but before submit;
11. after submit intent but before broker response.

Required result at every boundary:

- no duplicate broker call;
- no ownerless durable retry;
- restart converges to the same economic attempt;
- no illegal lifecycle jump;
- no silent terminalization of a valid retry.

---

## Data corruption matrix

Behavioral tests must cover:

- missing lifecycle metadata;
- malformed lifecycle metadata;
- blank lifecycle state;
- lifecycle column/meta disagreement;
- durable WATCHING but in-memory NONE;
- durable TRIGGER_READY but in-memory NONE;
- wrong client;
- wrong mode;
- PAPER/LIVE collision;
- stale generation;
- missing generation where current schema requires it;
- conflicting duplicate owner;
- whitespace/case variants;
- direction reversal;
- missing local order ID;
- wrong local order ID;
- terminal order with stale watcher;
- broker-ready order with stale watcher;
- retry timestamp in past/future/malformed;
- explicit zero attempt;
- negative attempt;
- non-integer attempt;
- conflicting watcher/materializer counters.

Every ambiguous identity/generation case must fail closed with zero broker mutation.

---

## Runtime / restart / deferred materializer parity

Build an explicit matrix with actual resolved values:

| Scenario | Execution core | Restart recovery | Deferred materializer / watcher |
|---|---|---|---|
| valid WATCHING | WATCHING | WATCHING | WATCHING |
| valid triggered retry | same generation | same generation | same generation |
| stale generation | HOLD | HOLD | HOLD |
| client mismatch | HOLD | HOLD | HOLD |
| mode mismatch | HOLD | HOLD | HOLD |
| ownership loss | no mutation | no mutation | no mutation |
| broker-ready | no re-trigger | handoff | no re-trigger |
| terminal | no callback | no rearm | remove owner |

No prose substitutes for this matrix.

---

## Readiness recovery requirements

The September 4 incident also showed readiness remaining degraded after the transient database problem.

Do not solve that by globally forcing `entries_allowed=true`.

Instead prove the exact dependency chain:

```text
readiness degraded reason
-> condition becomes healthy
-> authoritative inventory is reread
-> stale degraded reason clears only when its invariant is satisfied
-> valid owned retry is allowed to continue
```

Required tests include:

- DB unavailable -> readiness degraded -> DB healthy -> inventory proof healthy;
- stale `pending_trigger_without_watcher_ownership` clears only after exact owner proof;
- stale `watching_rows_missing_orders` clears only after exact order proof;
- a genuinely ownerless row remains blocked;
- LIVE/PAPER inventories cannot satisfy one another's readiness proof.

If #569 already owns a given readiness reason correctly after rebase, do not duplicate it here.

---

## Money-path safety

Required invariants:

- zero broker POST on all fail-closed lifecycle/recovery paths;
- exactly one canonical submit on the valid resumed path, and only through existing submit authority;
- no new cancel authority;
- no position mutation;
- no proof_trades write before fill;
- no queue/result corruption;
- no duplicate submit from watcher re-fire;
- no capacity replay that silently suppresses a trade because the same retry is counted twice.

Could this make Jason trade junk? **No**, if implemented correctly. It restores valid lifecycle ownership only. Score, contract quality, Master Control, sizing, and final risk gates still decide whether the resumed signal may trade.

---

## Test quality requirements

Primary proof must be executed behavior, not source-string assertions.

At least one production-shaped PostgreSQL/driver-faithful restart replay must reconstruct the watcher from durable rows rather than reusing the same object.

Inspect tests for camouflage:

- callback mock must actually be reached in positive control;
- negative tests need a valid positive control;
- no monkeypatch may bypass lifecycle validation;
- retry tests must advance time or durable next-retry authority realistically;
- attempt assertions must verify durable/reconstructed behavior, not just one object's attribute;
- broker mock must assert call count zero or exactly one as applicable.

---

## Expected production scope

Do not pre-authorize broad edits. Fail-first caller tracing decides the minimal scope.

Likely relevant seams:

- `ap_lifecycle.py` only if lifecycle restoration requires a canonical recovery API;
- `ap_entry_watcher.py` for callback retry ownership/counter behavior;
- `ap/pending_trigger_restart_recovery.py` for reconstruction caller wiring;
- `ap/order_monitor.py` only if broker-ready handoff still routes through the wrong consumer after #569;
- focused P0 tests and workflow registration.

Explicitly out of scope:

- Master Control thresholds;
- sector mapping (#548);
- contract selector quality policy;
- sizing;
- scanner admission;
- exit logic;
- broker submit/cancel semantics;
- proof taxonomy.

If the fix requires changing more than the proven caller chain, stop and amend the spec before broadening.

---

## Merge gate

Final implementation remains **HARD HOLD** until:

1. exact PEP fail-first loop is reproduced;
2. every changed production line is traced to callers/downstream mutations;
3. `NONE -> TRIGGER_READY` loop is eliminated for recovered valid owners;
4. watcher retry attempt semantics are durable and bounded or explicitly validity-bound, never `1/3` forever;
5. restart after every lifecycle boundary converges;
6. #569 and #568 interactions are tested after rebase;
7. readiness recovery clears only stale infrastructure blocks, not real safety failures;
8. stale generation/ownership/client/mode tests fail closed;
9. zero broker calls on fail-closed paths;
10. exactly one submit on the valid resumed path;
11. no trade-quality/risk threshold changes;
12. exact-head P0 CI green;
13. independent final audit gives MERGE.
