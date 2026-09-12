# P0 — Guarantee overnight watcher ownership before the 09:30 ET market open

## Status

**SPEC FIRST / DRAFT / HARD HOLD / IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY FROM THIS SPEC-ONLY HEAD.**

Repository: `angelprecision/angel_precision_bot`

Base: `main@16564d7e9df6fd1b4320c76173446c33891a994f`

Audit date: `2026-09-01`

This PR exists to fix one production trade-flow failure class observed on Jason LIVE on 2026-09-01:

> valid overnight Daily setups are first obtaining durable order/watcher ownership after the 09:30 ET open, after the underlying has already moved through trigger/target geometry, causing otherwise valid opportunities to terminalize as late attachment / target already complete instead of being watched from the open.

Do not broaden this PR into selector, scoring, retry-authority, broker-submit, exit, proof, queue, scanner, or risk redesign.

---

# Production incident

Jason LIVE produced substantial overnight entry inventory on 2026-09-01, but only IWM reached broker fill.

Observed production shape:

- 69 Jason ENTRY orders were created between approximately **09:31:54 ET and 09:34:03 ET**.
- 1 filled: IWM PUT.
- 23 remained `PENDING_TRIGGER` at the audit snapshot.
- 23 were canceled.
- 22 expired.

Important terminal/rejection families included:

- `target_already_complete_terminal`: 13
- `late_attachment_move_missed_terminal`: 7
- `RETRY_MAX_ATTEMPTS_EXCEEDED`: 5
- `CURRENT_PRICE_FETCH_FAILED` retry exhaustion: 2
- `restart_stuck_trigger_ready_no_broker_proof`: 2
- `MATERIALIZATION_IN_FLIGHT`: 1
- `SPREAD_TOO_WIDE`: 10
- `EARNINGS_LOCKOUT`: 3
- `NO_VALID_PLAYBOOK_DTE_CONTRACT`: 1
- `UNTRADEABLE_FOR_ACCOUNT_SIZE`: 1

The selector-quality and safety rejections are not the target of this PR.

The target is the timing/ownership class represented by rows first materialized/attached after the open and then rejected because the move was already underway or complete.

Concrete production example:

- LCID PUT trigger: 4.82
- watcher attachment/current underlying around 4.735
- watcher terminalized as `late_attachment_move_missed_terminal`

The watcher safety classifier is behaving correctly once it sees a late attachment. The defect is that a healthy overnight setup reached first-time watcher attachment too late.

Positive control:

- IWM LIVE PUT
- timeframe: `1D`
- STRAT pattern: `3-2-2`
- score: 72 / tier B
- trigger: 292.70
- selected contract: IWM 293 PUT
- submitted: about 09:32:58 ET
- broker fill: about 09:32:59 ET at 1.04

IWM proves the downstream breach -> deferred selection -> LIVE gates -> broker submit -> fill path can work when the setup survives long enough to reach it.

---

# Root invariant

For each eligible overnight Daily setup assigned to a LIVE client:

```text
before the market opens
-> exact source identity is inventoried
-> exact deferred ENTRY identity exists durably when appropriate
-> exact watcher ownership exists in the current process
-> readiness can prove that ownership
```

By the pre-open deadline, every eligible candidate must belong to exactly one of these classes:

```text
A. durably owned PENDING_TRIGGER watcher
B. legitimate terminal strategy/safety rejection
C. explicit retryable DATA/INFRA failure that keeps LIVE readiness fail-closed
D. already-resolved canonical lifecycle
```

Never:

```text
healthy eligible overnight candidate
-> no runtime watcher at 09:30
-> first attachment at 09:31-09:34
-> market already moved
-> late_attachment_move_missed_terminal / target_already_complete_terminal
```

---

# Architectural boundary

Current intended path is conceptually:

```text
scanner signal from prior session
-> WATCHING
-> overnight reevaluation
-> validation
-> Master Control
-> create deferred ENTRY / PENDING_TRIGGER
-> entry_watcher.watch(plan, local_order_id)
-> morning handoff / recovery verifies ownership
-> market opens
-> watcher polls underlying quote
-> breach confirmation
-> deferred contract materialization
-> current selector quality gates
-> current LIVE submit gates
-> canonical broker submit
```

This PR owns only the pre-open portion through proven watcher ownership.

It must not create a second entry lifecycle.

---

# Required implementation work

## 1. Trace the exact current-main caller chain before editing

Claude must first identify and document the real current-main path for:

```text
ClientRunner scheduler/startup
-> run_overnight_reeval_attempt
-> run_overnight_reeval
-> source inventory
-> overnight validation
-> Master Control decision
-> create_entry_order / existing order recovery
-> entry_watcher.watch(...)
-> post-overnight morning handoff
-> startup recovery / watcher reseed
-> preopen readiness
```

For every changed production function, provide:

```text
caller
-> validation
-> state read
-> authority resolution
-> durable mutation
-> runtime ownership mutation
-> return classification
-> downstream consumer
```

Do not patch a helper in isolation.

## 2. Fail-first reproduce the 2026-09-01 timing defect

Before production edits, add an executed behavioral regression that proves current main can produce this shape:

```text
valid prior-session Daily setup
+ healthy client
+ market day
+ overnight workflow does not finish watcher ownership before open
+ first watcher admission occurs after 09:30
+ current quote has already decisively passed entry geometry
-> terminal late attachment / target complete
```

Use realistic source/order metadata:

- exact `client_id`
- `execution_mode='live'`
- canonical signal identity
- real deferred order identity
- Daily timeframe
- CALL or PUT side
- trigger, stop, target
- timezone-aware ET clock

Do not prove this with `inspect.getsource()` alone. Structural tests may remain secondary.

## 3. Establish complete pre-open source inventory

The overnight runner must be able to prove whether it has fully inventoried the eligible source rows for the client/session.

Do not rely only on aggregate counts such as `fetched=69` or `armed=69`.

Each source candidate must retain enough durable identity to prove which order/watcher disposition represents it.

At minimum preserve exact:

- source table/type when multiple sources exist
- source job/row identity
- `signal_id`
- canonical signal identity
- `client_id`
- `execution_mode`
- ticker
- side/direction
- timeframe
- pattern when available
- source created/session timestamp

Two candidates may not satisfy each other's readiness merely because they share a ticker.

## 4. Materialize deferred ENTRY ownership before 09:30

A Daily overnight setup must not require an OCC contract before watcher ownership.

Preserve the existing deferred model:

```text
pre-open
-> DEFERRED:<ticker> / equivalent deferred ENTRY identity
-> PENDING_TRIGGER
-> watcher owned

breach-time
-> fresh option-chain lookup
-> contract selection
-> spread/OI/volume/delta/premium/DTE checks
-> broker-ready
```

Do not preselect stale overnight OCC contracts merely to claim readiness.

## 5. Make watcher ownership provable before the deadline

For every nonterminal eligible candidate, readiness must prove an exact runtime watcher corresponding to the exact durable lifecycle.

Required identity fence:

```text
client_id
+ normalized execution_mode
+ canonical_signal_id
+ local_order_id
```

If current architecture includes a durable generation/owner field for this lifecycle, preserve and validate it. Do not fabricate a generation where the ordinary watcher path does not own one.

Missing, blank, malformed, cross-mode, cross-client, or conflicting identity fails closed.

## 6. Pre-open readiness deadline

Implement or reuse one canonical readiness deadline immediately before market open. Preferred target for tests: **09:29:30 ET** or the closest existing canonical pre-open deadline that safely provides the same guarantee.

At/after that deadline, LIVE entry readiness may be `OK` only when every eligible source candidate is exactly accounted for by:

```text
watcher-owned PENDING_TRIGGER
+ legitimate terminal disposition
+ already-resolved canonical lifecycle
```

Any retryable infrastructure/data candidate without proven watcher ownership must keep LIVE entry readiness blocked/degraded rather than disappear from the accounting.

Do not globally unblock LIVE because most rows are owned.

## 7. No first-time healthy watcher installation after open

Add a behavioral invariant:

```text
if the candidate was eligible before open
and no explicit retryable failure blocked readiness
then the first successful watcher installation must occur before 09:30 ET.
```

After 09:30, recovery may restore an already-durable owner after process restart, but it must not pretend a never-owned row was being watched before the open.

## 8. Restart behavior must match uninterrupted runtime

Required matrix:

| Scenario | Uninterrupted runtime | Restart recovery | Morning handoff |
|---|---|---|---|
| Eligible row at 09:20 | watcher owned before open | same | same |
| Restart at 09:27 with durable PENDING_TRIGGER | watcher restored before open | exact same order | no duplicate |
| Restart at 09:31 with proven pre-open durable watcher lifecycle | may restore same lifecycle | no new order | no fabricated trigger history |
| Restart at 09:31 with no durable pre-open ownership | fail closed / missed | fail closed | no invented prior ownership |
| Cross-client row | no ownership | no ownership | no ownership |
| PAPER row on LIVE runner | no ownership | no ownership | no ownership |
| malformed identity | blocked | blocked | blocked |

Runtime, restart, and handoff must resolve the same authority from the same persisted facts.

## 9. Preserve the late-attachment safety classifier

Do not weaken or remove:

- `late_attachment_move_missed_terminal`
- `target_already_complete_terminal`
- `stop_already_broken_terminal`

Those remain correct when a row truly arrives late.

The acceptance result is fewer healthy-startup rows ever reaching those late-attachment states, not a change in what the states mean.

## 10. Preserve breach-time quality gates

Do not change:

- score floor
- scanner logic
- STRAT patterns
- intelligence admission mode
- spread gate
- OI / volume gate
- delta gate
- premium gate
- DTE policy
- earnings lockout
- account affordability
- max positions
- risk sizing

PDD on 2026-09-01 is a negative control: it reached breach processing and was rejected for `SPREAD_TOO_WIDE`. That remains a valid no-trade outcome.

---

# Failure-class audit requirements

Review by failure class, not by blocker number.

## Authority

Prove exact authority for:

- source inventory identity
- local order identity
- canonical signal identity
- watcher ownership
- execution mode
- client identity
- any generation/lease fields actually used
- readiness completion state

## State transitions

Execute:

- initial pre-open arm
- already-armed idempotent rerun
- morning handoff
- startup restart
- restart before open
- restart after open
- ownership loss
- conflicting watcher
- source terminalized before arm
- valid breach after pre-open ownership

## Failure timing

Inject failure:

- before source inventory completes
- after source read, before order creation
- after order creation, before watcher installation
- after watcher installation, before readiness persistence
- after readiness persistence, before market open
- process death at each boundary

No boundary may yield duplicate order creation or fabricated ownership.

## Data corruption

Test:

- missing `client_id`
- malformed execution mode
- blank execution mode
- column/meta execution-mode contradiction
- missing signal ID
- conflicting canonical signal ID
- missing local order ID
- stale source row
- wrong trading date
- malformed source payload
- duplicate candidate identities
- duplicate same-ticker opposite-side candidates

## External authority

Where broker/market data is consulted pre-open:

- transport unavailable != valid empty truth
- stale quote != current breach truth
- no current quote should be used to fabricate historical trigger crossing

## Money-path safety

For every fail-closed pre-open path prove:

- **zero broker ENTRY POST**
- **zero broker EXIT POST**
- **zero broker cancel introduced by this PR**
- no position creation before broker-confirmed fill
- no `proof_trades` write before fill
- no queue/result corruption
- no LIVE/PAPER cross-contamination

Valid breach path must still produce at most one canonical broker ENTRY submit.

---

# Required behavioral tests

At minimum add executed tests for all of these.

## Positive controls

1. **IWM-shaped Daily PUT**
   - source available pre-open
   - deferred order created before open
   - watcher owned before open
   - 09:30+ quote breaches trigger
   - current breach confirmation runs
   - deferred selector is invoked only at breach
   - valid contract may proceed to current canonical submit path

2. **No-trigger control**
   - watcher owned before open
   - quote never breaches
   - remains PENDING_TRIGGER
   - zero broker POST

3. **Legitimate quality rejection**
   - watcher owned before open
   - trigger breaches
   - selector returns `SPREAD_TOO_WIDE`
   - no broker POST
   - no attempt to bypass quality for throughput

## Timing controls

4. first-time arm completed at 09:29:29 -> valid
5. first-time healthy arm delayed past deadline -> LIVE readiness BLOCKED, not silently OK
6. quote crosses at 09:30:01 while pre-open owner exists -> watcher consumes current quote
7. quote already beyond terminal geometry when a genuinely late unowned recovery arrives -> existing terminal late-attachment classifier still fires

## Restart controls

8. process death after durable order creation but before in-memory watcher install
9. process death after watcher-install marker / ownership persistence
10. restart 09:27 -> same local order, one watcher, no new order
11. restart 09:31 with proven existing lifecycle -> restore exact lifecycle only
12. restart 09:31 without proven lifecycle -> no fabrication, zero POST

## Identity controls

13. wrong client
14. LIVE/PAPER mismatch
15. wrong local order ID
16. canonical-signal mismatch
17. same ticker/opposite sides remain independent
18. duplicate source alias cannot double-count readiness

## Failure controls

19. source DB read error
20. order persistence error
21. watcher install raises
22. readiness persistence error
23. market-data unavailable pre-open

Every failure must have a visible reason and preserve fail-closed broker behavior.

---

# Test-quality rules

Do not use source inspection as primary proof.

Review tests for encoded bugs:

- no assertion that merely preserves old 09:31+ batching behavior
- no mock that bypasses the real order persistence seam being tested
- no test that declares success because watcher callback was never reached
- negative tests need positive controls
- restart tests must actually reconstruct state rather than reuse the same in-memory object
- no broad monkeypatch that disables readiness or identity guards
- exact call counts must correspond to semantic authority, not current incidental implementation

A green test is not proof if it never executes the production path.

---

# Preferred file scope

Start by attempting the fix within:

```text
ap_overnight_reeval.py
client_runner.py
ap/morning_handoff.py          # only if exact ownership/readiness consumer requires it
ap_recovery.py                 # only if restart parity requires it
```

Focused tests may add new files and register them in P0 CI.

Default expectation:

- **do not materially change `ap_entry_watcher.py`**
- do not change `ap_execution_core.py` unless fail-first proves current caller wiring itself is the timing defect
- do not change OSM submit/cancel behavior

If implementation needs more production files, stop and explain the exact caller requirement before broadening.

---

# Explicit non-scope

This PR must not fix or absorb:

- retry-attempt authority conflicts (`RETRY_MAX_ATTEMPTS_EXCEEDED` with contradictory durable counters)
- `CURRENT_PRICE_FETCH_FAILED` retry redesign
- #560 safe-partial readiness semantics wholesale
- old #519 retry-counter implementation wholesale
- #561/#562 broker submit-intent reconciliation
- #566/#532 broker-flat exit fill adoption
- #516/#517 exit ownership
- exits or pullback logic
- proof taxonomy
- queue terminal cleanup
- scanner expansion
- new intraday scanners
- score/risk policy changes
- capital policy

Those are separate PRs.

---

# Existing PR relationships

## PR #560

Do **not** merge or cherry-pick #560 as the fix for this incident.

#560 re-runs post-overnight handoff/readiness when a completed source inventory leaves safely owned retryable deferrals. Its actual diff does not guarantee that first-time watcher ownership is established before 09:30.

Reuse only a narrowly proven concept if current main needs it.

## Old PR #519

Do not merge/cherry-pick #519 into this PR. Retry-attempt authority is a separate failure class and should be rebuilt independently on current main after this timing fix.

## PR #566

Independent exit-truth work. It does not fix entry trade flow and should not be stacked into this branch.

---

# Required observability

Add stable diagnostics sufficient to answer these questions from production logs without database archaeology:

```text
How many eligible overnight candidates were inventoried?
How many were watcher-owned by the pre-open deadline?
Which exact source identities were not owned?
When was each watcher's first successful installation?
Was an installation normal, handoff, or restart recovery?
Did readiness block because ownership was missing?
```

Preferred event family (names may adapt to existing conventions):

```text
PREOPEN_WATCHER_INVENTORY
PREOPEN_WATCHER_OWNED
PREOPEN_WATCHER_OWNERSHIP_MISSING
PREOPEN_WATCHER_READINESS_BLOCKED
PREOPEN_WATCHER_READINESS_OK
```

Diagnostics must include exact client/mode/order/signal/ticker identity where applicable.

Do not make diagnostics trading authority.

---

# Production acceptance gate

After deployment to a controlled LIVE acceptance window, require:

```text
eligible overnight rows without disposition by pre-open deadline = 0
PENDING_TRIGGER rows without exact watcher owner at open = 0
healthy first-time overnight watcher attachments after 09:30 = 0
late_attachment_move_missed_terminal caused by healthy startup timing = 0
overnight watch-arm failures caused by missing ownership timing = 0
broker calls on fail-closed pre-open paths = 0
broker ENTRY submit for one valid generation <= 1
LIVE/PAPER identity crossings = 0
```

Do not define success as a guaranteed number of trades. Market/contract quality remains authoritative.

The engineering success criterion is:

> if the market produces a valid eligible breach, infrastructure timing must not be the reason Angel Precision misses it.

---

# Implementation sequence for Claude

1. Read this spec in full.
2. Read PR description.
3. Read current-main code, not stale PR branches.
4. Trace exact caller chain and list actual production files/functions.
5. Reproduce fail-first timing defect with executed behavioral test.
6. Implement the smallest pre-open ownership correction.
7. Add restart parity tests.
8. Add identity/corruption tests.
9. Add money-path zero-POST negative controls and one valid-path positive control.
10. Register focused tests in blocking P0 CI if not already included.
11. Run focused suites.
12. Run adjacent overnight/handoff/recovery/watcher/deferred suites.
13. Run exact-head blocking P0 CI.
14. Audit complete diff, not just final amendment.
15. Report final changed files and caller -> mutation -> consumer trace.
16. Do **not** merge or deploy.

---

# Required final report from Claude

Before requesting review, post:

## Exact base/head

```text
base SHA
head SHA
changed-file count
production files
behavioral test files
```

## Fail-first evidence

Exact failing test/result against unmodified current main.

## Caller trace

For every changed production function:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

## Runtime/restart/handoff matrix

Show resolved values, not prose-only claims.

## Money-path audit

Explicit counts/new call sites for:

- broker submit
- broker cancel
- orders mutations
- positions mutations
- proof_trades mutations
- queue mutations

## Test evidence

Executed behavioral tests with counts.

## Residual risks

Anything not solved must be called out explicitly and left for a separate PR.

---

# Merge gate

**HARD HOLD until implementation exists and exact-head evidence is reviewed.**

After implementation, final verdict may be `MERGE` only if all are true:

- healthy eligible overnight candidates are provably watcher-owned before open;
- no first-time healthy watcher installation is deferred to 09:31+;
- restart cannot fabricate prior ownership;
- uninterrupted/restart/handoff behavior agrees;
- existing late-attachment safety remains intact;
- selector/risk/scoring/earnings/spread gates are unchanged;
- no new broker submit/cancel path exists;
- no pre-fill positions/proof mutation exists;
- exact client/mode/order/signal identity is preserved;
- blocking P0 CI is green on exact head;
- complete final diff audit finds no hidden production-shape issue.

**HOLD** if implementation is incomplete but no money-path hazard exists.

**HARD HOLD** if any path can:

- create duplicate ENTRY orders;
- submit before exact watcher/breach authority;
- cross LIVE/PAPER identity;
- fabricate pre-open ownership after the fact;
- weaken late-attachment safety to chase already-completed moves;
- mark readiness OK while eligible candidates lack proven ownership;
- add unreviewed broker submit/cancel authority.
