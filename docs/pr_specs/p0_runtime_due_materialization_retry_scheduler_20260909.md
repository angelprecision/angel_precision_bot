# P0 SPEC — Runtime scheduler for due deferred materialization retries

**Status:** SPEC ONLY / DRAFT / HARD HOLD / DO NOT MERGE / DO NOT DEPLOY

**Base:** `main@d404df34e00522ba2cce995b6a0129ba39b83944`

## Why this PR exists

Production on 2026-09-09 proves the deferred-materialization retry system can correctly persist retry authority but still fail to execute that retry when it becomes due during the trading session.

Current LIVE Jason evidence from Supabase:

```text
local_order_id: 30a7ec8e-7c7a-4bf3-b7be-d73134c534d1
client_id: jasoncosby1@gmail.com
execution_mode: live
symbol: CCEP
contract: DEFERRED:CCEP
status: PENDING_TRIGGER
lifecycle_state: RETRY_WAIT
materialization_status: RETRY_PENDING
materialization_next_retry_at: 2026-09-09T14:09:23.753928+00:00
next_retry_at: 2026-09-09T14:09:23.753928+00:00
materialization_generation: 7
retry_attempt: 2
breach_attempt_count: 2
materialization_attempts: 2
retry_owner: watcher:71e72735-71b2-4370-9f13-16e06a6f0d2c
materialization_retry_owner: <blank>
broker_ready: false
broker_order_id: null
submitted_ts: null
```

The row remained `RETRY_PENDING` after its retry timestamp was due.

This is not a selector-quality issue and is not permission to weaken spread, OI, volume, delta, DTE, moneyness, score, sizing, capacity, account, or risk gates.

## Confirmed architecture on current main

Current main already contains the canonical deferred retry executor:

```text
APStartupRecovery._recover_deferred_breach_lifecycles()
  -> durable due-retry query
  -> exact identity / mode / retry / broker-evidence gates
  -> APExecutionCore.resume_deferred_materialization_retry()
  -> existing durable materialization claim/CAS
  -> selector/materializer
  -> existing OSM broker-ready copyback / retry / terminal / HOLD outcome
```

Production-shaped replay has already shown that this path can execute successfully on current main when invoked. Therefore this PR must **not** create a second retry executor.

The missing production capability is runtime invocation after startup/morning recovery has already completed.

## Root failure class

The canonical executor is wired into startup/morning recovery, but a retry can be created later in the session and become due after those one-shot recovery phases have passed.

Failure sequence:

```text
runner starts
-> startup recovery executes
-> later, watcher confirms breach
-> selector/materialization attempt returns retryable outcome
-> durable RETRY_WAIT / RETRY_PENDING is persisted
-> next_retry_at becomes due
-> no runtime consumer invokes the canonical due-retry executor
-> order monitor may observe/classify the row, but does not own retry execution
-> row remains RETRY_PENDING indefinitely
```

This PR must solve only the missing runtime scheduling/invocation seam.

## Binding invariant

For each active client runner and execution mode:

```text
exact PENDING_TRIGGER durable row
+ confirmed trigger authority
+ canonical RETRY_PENDING / RETRY_WAIT authority
+ timezone-aware retry timestamp now due
+ valid generation / attempt lineage
+ attempts remain
+ entry deadline remains valid
+ no broker handoff / submit-intent evidence
-> runtime scheduler invokes the EXISTING canonical deferred retry executor
-> exactly one durable claimant wins
-> exactly one selector/materialization attempt occurs
-> row advances to one canonical outcome:
   BROKER_READY
   OR newer RETRY_PENDING/RETRY_WAIT
   OR honest terminal outcome
   OR HOLD
```

A due retry must not remain unchanged merely because the client runner did not restart again.

## Ownership

### Scheduler owner

The runtime scheduler should be owned by `ClientRunner` or an existing client-runner lifecycle component that already owns the live per-client execution stack.

Why:

- it has exact client identity;
- it has exact execution mode;
- it owns the running broker/OSM/execution-core stack;
- it already constructs/uses `APStartupRecovery`;
- it can invoke the existing canonical recovery boundary without giving broker or retry authority to `APOrderMonitor`.

### Executor owner

The executor remains the existing recovery/materialization path. Do not duplicate it.

Expected call shape should stay conceptually equivalent to:

```text
ClientRunner runtime tick
  -> narrow due-retry recovery entry point
  -> APStartupRecovery._recover_deferred_breach_lifecycles()
     or a minimal public wrapper around only this existing method
  -> APExecutionCore.resume_deferred_materialization_retry()
```

If a new public wrapper is needed, it must be a thin delegator only. It must not contain duplicate retry classification, duplicate CAS logic, duplicate selector invocation, or duplicate broker authority.

## Explicit non-goals

Do NOT:

- add a second due-retry executor;
- call `resume_deferred_materialization_retry()` from `APOrderMonitor`;
- make order monitor a scheduler;
- add new retry lifecycle states;
- add new owner concepts;
- add new durable retry columns unless fail-first evidence proves current authority is insufficient;
- add new broker submit authority;
- add new broker cancel authority;
- bypass or weaken existing generation CAS;
- bypass exact client identity checks;
- bypass execution-mode checks;
- synthesize missing durable identity from runner context;
- weaken broker-evidence fencing;
- treat UNKNOWN broker truth as broker absence;
- modify selector quality policy;
- modify scanner logic;
- modify score/tier thresholds;
- modify sizing/capacity/risk policy;
- absorb #580 watcher-lifecycle work;
- absorb #597 terminal durable-row/watcher convergence;
- absorb #569 pre-open ownership/readiness;
- absorb #595 overnight durable-attempt/readiness authority;
- revive old #596 wholesale;
- merge #602 into this PR.

## Expected production scope

Target the smallest possible production diff.

Preferred production files:

```text
client_runner.py
ap_recovery.py   # only if a narrow public runtime entry point is required
```

No other production file should change without fail-first proof that the runtime scheduler cannot be implemented safely inside this boundary.

If any additional production file is changed, the PR description must explain exactly why, with caller-to-side-effect trace.

## Runtime cadence

The scheduler must run repeatedly while the client runner is alive during the relevant entry window.

Requirements:

- configurable interval;
- do not exceed broker or DB safety expectations;
- suggested default: 15-30 seconds unless existing runner cadence provides a cleaner reuse point;
- no busy loop;
- no per-row thread creation;
- one client runner must not process another client's retry rows;
- scheduler failure must be visible in logs/telemetry and must not silently kill the runner;
- scheduler must stop cleanly when the runner stops;
- scheduler must not run after shutdown authority is set;
- scheduler must not mutate unrelated startup recovery state every interval.

Prefer a narrow due-retry tick over re-running full startup recovery repeatedly.

## Fail-first production replay

Before implementing the fix, add a production-shaped test that fails on committed main.

Seed the exact current LIVE CCEP shape:

```text
client_id = jasoncosby1@gmail.com
execution_mode = live
status = PENDING_TRIGGER
symbol = CCEP
contract = DEFERRED:CCEP
lifecycle_state = RETRY_WAIT
materialization_status = RETRY_PENDING
materialization_generation = 7
retry_attempt = 2
breach_attempt_count = 2
materialization_attempts = 2
next_retry_at = already due
broker_ready = false
broker_order_id = null
submitted_ts = null
```

The test must prove:

1. runner startup recovery has already completed;
2. the retry becomes due during runtime without restarting the runner;
3. committed main leaves the row unchanged because no runtime consumer invokes the canonical executor;
4. after the implementation, the runtime tick invokes the existing canonical due-retry path;
5. exactly one selector/materialization attempt occurs;
6. the row advances to a canonical outcome.

Do not hand-call `resume_deferred_materialization_retry()` in the positive runtime-scheduler test. The test must begin at the actual runtime scheduler/caller added by this PR.

## Required positive controls

### A. LIVE CCEP runtime-due replay

Exact current incident shape.

Expected:

```text
runtime tick
-> exact client/mode query
-> due row discovered
-> existing recovery authority checks
-> existing CAS claim
-> selector/materializer exactly once
-> canonical result
```

### B. LIVE MO replay

Seed prior September LIVE MO retry shape and prove the same runtime path executes without runner restart.

### C. PAPER MMM replay

Seed PAPER MMM retry shape and prove execution-mode isolation remains correct.

### D. WFC existing-success control

Existing WFC deferred retry behavior must remain unchanged.

## Required race proofs

### 1. Two runtime ticks racing

Two scheduler invocations observe the same due row.

Must prove:

- one durable claim winner;
- one selector invocation;
- one materialization attempt;
- losing worker performs zero broker submit;
- losing worker performs zero broker cancel;
- losing worker performs zero proof-trade write;
- losing worker performs zero position mutation;
- losing worker performs zero destructive queue mutation.

### 2. Startup recovery racing runtime scheduler

A runtime retry becomes due while startup/morning recovery is also invoking the same canonical consumer.

Must prove existing generation/attempt CAS prevents duplicate execution.

No new process-local mutex may be treated as the safety authority. Durable CAS remains authoritative.

### 3. Watcher callback racing runtime retry

Watcher remains active while a due retry scheduler fires.

Must prove there is still exactly one materialization owner and no duplicate selector/broker path.

## Required crash/restart proofs

1. process dies immediately before runtime due-retry invocation;
2. process dies after durable claim but before selector result;
3. process dies after selector result but before durable broker-ready copyback;
4. process dies after broker-ready copyback but before any later submit seam;
5. restart re-enters through existing canonical recovery without duplicate selector or broker submit.

Do not invent a parallel crash-recovery mechanism. Reuse existing durable generation/attempt/broker evidence.

## Required negative controls

Each must prove zero selector and zero broker/position/proof mutation unless the existing canonical implementation intentionally terminalizes the row.

- future `next_retry_at`;
- missing retry timestamp;
- malformed timestamp;
- timezone-naive timestamp if current canonical policy rejects it;
- client mismatch;
- execution-mode mismatch;
- missing client identity;
- missing local order identity;
- signal identity mismatch;
- stale materialization generation;
- conflicting retry-attempt aliases;
- malformed retry attempt;
- attempts exhausted;
- entry deadline expired;
- broker_order_id present;
- submitted_ts present;
- durable submit-intent evidence present;
- broker truth UNKNOWN;
- active conflicting materialization owner;
- malformed owner/lease authority;
- row no longer `PENDING_TRIGGER`;
- row already `BROKER_READY`;
- row already terminal;
- shutdown/runner-stop in progress.

## Side-effect ordering

The scheduler itself owns no money-path side effects.

Before durable claim winner:

```text
broker submit = 0
broker cancel = 0
position mutation = 0
proof_trades = 0
queue mutation = 0
```

After durable claim winner, all behavior must remain delegated to the existing canonical executor.

The new scheduler must never directly submit/cancel a broker order.

## Query requirements

Any new runtime query must be narrow and exact:

- exact client_id;
- exact execution_mode where stored/required;
- ENTRY only;
- PENDING_TRIGGER only;
- canonical retry lifecycle/status only;
- due timestamp only;
- exclude obvious broker-handoff rows where the existing canonical query already does so;
- bounded result count if necessary for safety.

Prefer reusing the query inside `APStartupRecovery._recover_deferred_breach_lifecycles()` rather than implementing a second SQL query in `ClientRunner`.

ClientRunner should schedule the canonical consumer, not become a second authority resolver.

## Observability

Add narrow runtime visibility sufficient to prove the scheduler is alive without log flooding.

At minimum record/emit:

- scheduler tick started or summarized at bounded cadence;
- client_id;
- execution_mode;
- due rows found;
- rows claimed/executed;
- rows held/skipped;
- canonical disposition counts;
- errors;
- current commit SHA if existing runner logging already carries it.

Do not emit one noisy line every few seconds for zero-work ticks unless debug-level and rate-limited.

A stranded retry should be diagnosable from production logs without querying the database manually.

## Liveness watchdog

Add a diagnostic-only liveness assertion/telemetry path for rows that remain due beyond a bounded grace window.

Example concept:

```text
RETRY_PENDING
+ next_retry_at < now - grace
+ no broker evidence
-> emit DUE_RETRY_STALLED diagnostic
```

This diagnostic must not create broker or cancellation authority and must not bypass canonical retry gates.

If equivalent telemetry already exists, reuse it.

## Test quality requirements

Do not satisfy this PR with source inspection or stitched mocks alone.

Required behavioral layers:

1. real PostgreSQL fail-first runtime scheduler replay;
2. real ClientRunner/runtime-caller boundary;
3. existing APStartupRecovery due-retry consumer;
4. existing durable OSM CAS;
5. real execution-core retry method;
6. selector seam may be deterministic/test-controlled, but the test must prove it is invoked only through the real executor;
7. zero-side-effect assertions on every losing/negative path.

A test that directly calls the recovery function does not prove the scheduler exists.

A test that directly calls the materializer does not prove runtime liveness.

A test that merely asserts `RETRY_OWNED` classification does not prove execution.

## CI gate

Before review:

- focused runtime scheduler tests green;
- existing deferred retry ownership tests green;
- existing restart recovery tests green;
- watcher/materialization ownership tests green;
- exact-head canonical P0 inventory green;
- pull_request merge-ref canonical P0 inventory green;
- exact-head SHA attested in logs;
- merge-ref SHA attested in logs;
- `git diff --check` clean.

Exact-head and merge-ref jobs must run the same canonical P0 inventory.

## Backwards audit required before merge consideration

Audit every changed production line through:

```text
caller
-> scheduler timing
-> durable read
-> client/mode authority
-> retry authority
-> generation/attempt CAS
-> selector/materializer
-> OSM copyback
-> downstream consumer
```

Then compare:

- startup-only path;
- morning-handoff path;
- new runtime path;
- LIVE;
- PAPER;
- uninterrupted process;
- restart;
- concurrent startup/runtime invocation;
- concurrent watcher/runtime invocation.

Specifically audit for:

- duplicate selector calls;
- duplicate submit intent;
- duplicate broker submission;
- stale generation execution;
- process-local lock dependence;
- mode bleed;
- cross-client rows;
- shutdown races;
- runner thread leaks;
- scheduler dying silently after first exception;
- accidental full-startup-recovery replay on every tick;
- capacity/readiness mutation unrelated to due retry;
- order-monitor authority creep.

## Relationship to existing PRs

### #596

Do not merge/rebase old #596 wholesale. It contains a broader historical implementation against an older main. This PR extracts only the still-proven missing production capability: recurring runtime invocation of the already-existing canonical executor.

### #602

#602 is an order-monitor projection correction only. It is not the due-retry liveness fix and must not be merged into this scheduler PR.

Do not depend on #602 for runtime execution.

### #580 / #597 / #569 / #595

Keep their ownership boundaries intact. This PR must not absorb watcher restoration, terminal watcher convergence, pre-open readiness, or overnight attempt authority.

## Merge rule

**HARD HOLD / DO NOT MERGE / DO NOT DEPLOY** until:

1. committed-main fail-first proves the CCEP runtime scheduling gap;
2. implementation fixes it through the real ClientRunner runtime caller;
3. exact CCEP LIVE replay passes without runner restart;
4. LIVE MO and PAPER MMM runtime replays pass;
5. startup/runtime and watcher/runtime races prove exactly one durable winner;
6. crash/restart replay proves no duplicate selector/broker side effect;
7. exact-head and merge-ref P0 are green;
8. independent backwards audit finds no silent regression;
9. user explicitly authorizes merge.

Green CI alone is not merge permission.

## Codex implementation directive

Implement this spec narrowly on the existing branch/PR.

Start with fail-first tests on committed main. Do not edit production code until the fail-first runtime-scheduler replay demonstrates that a due retry created after startup remains stranded.

Then implement the smallest possible runtime scheduling seam, preferably in `ClientRunner`, that periodically invokes the existing canonical deferred due-retry recovery entry point.

Do not add retry business logic to the scheduler. Do not move retry authority into order monitor. Do not redesign materialization. Do not change selector policy. Do not change broker submission behavior.

When complete, report:

- exact base SHA;
- exact head SHA;
- exact production files changed;
- why each production line changed;
- fail-first evidence on committed main;
- CCEP LIVE runtime replay result;
- MO LIVE runtime replay result;
- MMM PAPER runtime replay result;
- WFC positive control;
- concurrency/race results;
- crash/restart results;
- zero-side-effect negative controls;
- focused test counts;
- exact-head P0 workflow run;
- merge-ref P0 workflow run;
- `git diff --check` result;
- explicit confirmation no #602 behavior, no second executor, no new broker authority, and no selector/risk-policy change.

Keep the PR Draft / HARD HOLD. Do not merge or deploy.