# P0 — Reconciler Canonical Exit-Owner Adoption (517-B)

## STATUS

**SPEC FIRST / DRAFT / HARD HOLD UNTIL IMPLEMENTED AND RE-AUDITED.**

This is the surgical replacement for **slice B** of old PR #517.

Slice A is PR #545 and owns one separate defect only:

```text
historical underlying_entry must be immutable
current market data must never become historical entry truth
```

This spec owns only:

```text
canonical DB position exists
+ exit engine may already hold a matching broker-repair owner
-> reconciler must adopt canonical exit ownership before generic seeding
```

Do not combine these concerns again.

---

## BASE / ORDERING

Spec branch created from:

```text
main@91965ab43cc3a3a3db0bbf13abd235a2c5ca651f
```

That historical spec base includes merged PR #544 and predates #545 and the
broker-recovery correction now merged through #585. The implementation is
rebased onto current main `9c719d72b2b9c3dba7f8c883d118142f48ce3246`,
which contains both dependencies.

### Required implementation ancestry

Implementation must be rebuilt/rebased from fresh current main **after**:

1. #545 is merged, because #545 owns reconciler historical-entry truth;
2. #585 is merged, because #585 supersedes #516 and owns broker-open / DB-lag canonical position identity and canonical owner convergence.

This PR remains Draft/HARD HOLD until those ancestry requirements are satisfied.

Do not copy #545 or #585 code into this PR. Rebase onto their merged commits.

### Post-rebase amendment boundary

The implementation also carries these narrowly scoped corrections required by
the #585 integration audit:

- A historical FILLED ENTRY with a blank `position_id` may be accepted only
  when client, mode, OCC, and an exact local/broker order linkage all agree;
  a nonblank conflicting predecessor ID remains a HOLD.
- Filled ENTRY lookup is enrichment-only. A complete canonical row may adopt
  through a temporary orders-read outage; lookup is required only for missing
  adoption metadata.
- Proven full and remaining quantities are passed through both adoption paths.
  A smaller in-memory remainder is retained fail-closed, while malformed or
  contradictory quantity truth is rejected.
- `NO_REPAIR_FOUND` seeding rechecks the exact client/mode/OCC domain under the
  exit-engine lock and proves the supplied object was registered. A degraded or
  conflicting owner blocks the generic add without takeover.
- If the atomic seed API is unavailable, the reconciler returns
  `atomic_seed_unavailable` and performs no generic `add_position()` fallback.
- P0 CI checks out and asserts the submitted PR head SHA, rather than relying
  on GitHub's synthetic pull-request merge ref.

---

# INCIDENT

## Jason LIVE NOW — canonical position appeared after synthetic exit ownership already existed

During the 2026-08-25 Jason LIVE NOW incident:

```text
client: jasoncosby1@gmail.com
mode: live
contract: NOW260828P00122000
broker ENTRY: 143201293
entry fill: 1 @ $1.30
canonical position_id: 2fe10f52-e459-4bc5-a57a-1a80e3618040
```

The exit engine first observed the broker-open contract during a DB timing gap.

The old broker-repair path produced an in-memory synthetic owner:

```text
broker-repair-jasoncosby1@gmail.com-NOW260828P00122000
```

Later, the canonical DB position existed.

The reconciler repeatedly reached the DB-open position and attempted to seed the exit engine through its normal reseed path:

```text
_reconcile_positions()
-> _seed_exit_engine_from_position(pos)
-> _seed_exit_engine_from_import(...)
-> exit_engine.add_position(mp)
```

That is the wrong authority seam when a `broker-repair-*` owner already exists.

The exit engine already exposes the dedicated canonical identity adoption API used by fill-monitor handoff:

```text
exit_engine.adopt_canonical_position_identity(...)
```

The fill-monitor path already treats that API as the ownership authority and handles structured outcomes before generic seeding.

The reconciler does not.

As a result, a canonical DB row can become visible while the behavior-active exit owner remains synthetic/degraded, or a second canonical object may be attempted through generic add/dedup semantics instead of explicit ownership convergence.

The production invariant must be stronger:

> One broker position, one exact client/mode/OCC domain, one behavior-active canonical exit owner.

---

# ROOT CAUSE

Current reconciler seeding treats every valid DB-open position approximately the same:

```text
canonical DB position found
-> construct ManagedPosition
-> add_position()
```

But there are two materially different states:

### State 1 — no prior repair owner exists

```text
canonical DB position
+ no matching broker-repair owner
-> normal canonical seed is appropriate
```

### State 2 — matching repair owner already exists

```text
canonical DB position
+ matching broker-repair owner already behavior-active
-> identity must be ADOPTED
-> generic add_position() is not the ownership authority
```

The current reconciler does not ask the exit engine which state it is in before calling generic add.

The fill-monitor already does.

This PR must reuse the existing adoption API rather than invent another reconciliation ownership system.

---

# EXISTING AUTHORITY TO REUSE

The current fill-monitor path already calls:

```python
exit_engine.adopt_canonical_position_identity(
    contract=...,
    canonical_position_id=...,
    local_order_id=...,
    broker_order_id=...,
    signal_id=...,
    canonical_signal_id=...,
    entry_fill=...,
    entry_ts=...,
    order_filled_ts=...,
    execution_mode=...,
    client_id=...,
    underlying_entry=...,
    score=...,
    tier=...,
    pattern=...,
    direction=...,
    timeframe=...,
    underlying_stop=...,
    underlying_target=...,
)
```

The existing fill-monitor disposition handling is the precedent:

```text
ADOPTED
ALREADY_CANONICAL_REPAIR_REMOVED
NO_REPAIR_FOUND
RETRY_*
```

This PR should use that API and outcome taxonomy.

### Forbidden architecture

Do not create:

- another adoption API;
- another synthetic-owner class;
- another ownership registry;
- another exit-engine wrapper;
- a second in-memory dedup subsystem;
- a new cross-module recovery framework.

The system already has the right primitive. The reconciler simply needs to use it correctly.

---

# REQUIRED PRODUCTION CHANGE

## Preferred production file

```text
ap_reconciler.py
```

Expected implementation should remain in this file.

### Do not touch `ap_exit_engine.py`

The existing adoption API is already live and used by fill-monitor. If implementation claims `ap_exit_engine.py` must change, stop and document the exact missing behavior with a fail-first test before modifying it.

Default assumption: **no exit-engine production changes are required.**

---

# PRIMARY TARGET SEAM

Trace:

```text
APBrokerReconciler._reconcile_positions()
-> _seed_exit_engine_from_position(pos)
-> _seed_exit_engine_from_import(...)
```

The canonical adoption decision must happen before the reconciler creates/adds another ManagedPosition for the same exact ownership domain.

Preferred placement is inside the canonical-position seeding path, not in generic broker-import persistence.

---

# BINDING BEHAVIOR

## Exact ownership domain

All adoption and postcondition checks must be fenced by:

```text
client_id
execution_mode
exact OCC contract
canonical position_id
```

When durable ENTRY evidence is used to enrich adoption metadata, it must additionally match the canonical position identity.

No ticker-only fallback.

No underlying-only ownership match.

No mode-agnostic match.

No cross-client match.

---

## Step 1 — validate canonical position identity

Before adoption:

```text
position_id must be nonblank/non-placeholder
client_id must be exact
execution_mode must normalize to live or paper
contract must be exact OCC
qty must be positive integral
entry option fill must be positive finite
```

Malformed or unknown mode fails closed.

The reconciler must not default missing execution mode from another account or from a broad runner assumption.

---

## Step 2 — collect only durable metadata already proven by existing lifecycle truth

Use the canonical DB position first.

Use exact filled ENTRY evidence only for missing fields that the adoption API needs.

Allowed durable fields include:

```text
canonical position_id
client_id
execution_mode
exact OCC
local ENTRY order id
broker ENTRY order id
signal_id / canonical_signal_id
confirmed entry fill price
confirmed fill timestamp
historical underlying_entry from #545 truth path
stop_underlying
target_underlying
score
tier
pattern
direction
timeframe
```

### Evidence precedence

For any identity-bearing field:

```text
canonical position value
+ exact ENTRY value
```

may fill a blank counterpart only when they do not conflict.

If both are nonblank and conflict:

```text
FAIL CLOSED
```

Do not choose newest.

Do not choose whichever source is convenient.

When the historical ENTRY predates canonical position creation, a blank
`ENTRY.position_id` is not itself a contradiction. It may be linked by the
exact local or broker ENTRY order ID inside the already fenced client/mode/OCC
domain. A nonblank predecessor ID that differs from the canonical UUID remains
an identity conflict.

If the canonical row already contains the required adoption fields, a temporary
ENTRY lookup outage must not block ownership convergence. Missing fill or
identity fields still require one exact, proven ENTRY lookup; a missing fill
timestamp alone may use the adoption API's deterministic fallback.

Do not overwrite a durable canonical value to make adoption succeed.

### Historical entry truth

#545 remains authoritative.

This PR must not reintroduce:

```text
current quote
current mark
current underlying
trigger price
opened_underlying heuristic
```

as historical `underlying_entry`.

If #545 resolves historical entry to:

```text
0.0 + untrusted
```

517-B must pass that state through honestly. Ownership adoption must not fabricate missing economics.

---

# ADOPTION OUTCOME MATRIX

## `ADOPTED`

Expected:

```text
matching broker-repair owner was converted to canonical identity
```

Required behavior:

```text
add_position() calls from reconciler = 0
```

Then verify postcondition:

```text
exact client/mode/OCC domain
-> exactly one behavior-active owner
-> owner.position_id == canonical position_id
-> no broker-repair-* owner remains in that exact domain
```

If postcondition fails, emit CRITICAL and return failure. Do not create another owner to compensate.

---

## `ALREADY_CANONICAL_REPAIR_REMOVED`

Expected:

```text
canonical owner already existed and stale repair owner was removed
```

Required behavior:

```text
add_position() calls from reconciler = 0
```

Run the same exact postcondition proof.

---

## `NO_REPAIR_FOUND`

This is the **only** adoption disposition that may fall through to the existing canonical seed path.

Required flow:

```text
adoption -> NO_REPAIR_FOUND
-> create/seed exactly one canonical ManagedPosition using existing reconciler code
-> add_position() at most once
-> verify exact canonical-owner postcondition
```

The seed helper must perform its final check under the same engine lock as the
add. It must re-check the exact client/mode/OCC domain immediately before the
add, refuse a newly appeared degraded/broker-repair or conflicting owner, and
return success only when the supplied canonical object is behavior-active,
indexed by its canonical ID, and is the sole exact-domain owner.

For partial exits, the adoption call receives both the proven original size and
the proven remaining size on `ADOPTED` and `ALREADY_CANONICAL_REPAIR_REMOVED`.
It may lower a stale remainder but must never increase it; a target whose full
size exceeds proven truth remains a retry/HOLD before cleanup.

Postcondition:

```text
exactly one behavior-active owner
exact client
exact mode
exact OCC
canonical position_id
no broker-repair-* owner in exact domain
```

If generic add returns without establishing that postcondition, treat the seed as failed.

A method returning successfully is not enough. Ownership state after the call is the truth.

---

## Any `RETRY_*`

Examples may include identity conflicts, ambiguous repair state, mode conflicts, or other retry dispositions returned by the existing API.

Required behavior:

```text
add_position() = 0
no second owner
no synthetic owner creation
no broker mutation
visible CRITICAL diagnostic
reconciler leaves recovery for a future poll / operator-visible resolution
```

**Never:**

```text
RETRY_*
-> generic add_position()
```

That recreates the exact ownership ambiguity this PR exists to remove.

---

## Adoption exception / unreadable outcome

Fail closed.

Required:

```text
add_position() = 0
broker mutation = 0
proof mutation = 0
queue mutation = 0
visible diagnostic
```

Do not treat an exception as `NO_REPAIR_FOUND`.

Do not assume the repair owner disappeared.

---

# REQUIRED POSTCONDITION HELPER

A narrow reconciler-local read-only helper is acceptable if needed.

It should inspect `exit_engine.active_positions()` and prove, for one exact domain:

```text
client_id == expected client
execution_mode == expected live/paper
option_symbol/contract == exact OCC
```

Success requires:

```text
count == 1
position_id == canonical position_id
no position_id startswith broker-repair-
```

Failure reasons should distinguish at minimum:

```text
OWNER_LOOKUP_UNAVAILABLE
OWNER_LOOKUP_FAILED
OWNER_COUNT_0
OWNER_COUNT_GT1
CANONICAL_OWNER_MISSING
BROKER_REPAIR_OWNER_PRESENT
OWNER_IDENTITY_CONFLICT
```

Do not mutate the engine from the postcondition helper.

Do not import fill-monitor private helpers just to reuse a few lines. Avoid circular authority coupling. Reproduce only the minimal read-only invariant in the reconciler if there is no public helper.

---

# REQUIRED FAIL-FIRST PRODUCTION REPRODUCTION

Before changing production code, add a focused test that reproduces current-main behavior after rebasing onto final #545 + #585 lineage.

Suggested file:

```text
tests/test_p0_reconciler_canonical_owner_adoption.py
```

## Fail-first setup

Create:

```text
canonical DB position:
  id = 2fe10f52-e459-4bc5-a57a-1a80e3618040
  client = jasoncosby1@gmail.com
  execution_mode = live
  contract = NOW260828P00122000
  qty = 1
  entry = 1.30

exit engine already contains:
  broker-repair-jasoncosby1@gmail.com-NOW260828P00122000
  same client
  same live mode
  same exact OCC
```

Drive the real reconciler seed path:

```text
_seed_exit_engine_from_position(canonical_pos)
```

Against unmodified implementation, prove that the reconciler does not explicitly route through canonical adoption before generic add.

Record exact pre-fix failure in the implementation report.

---

# REQUIRED POSITIVE CONTROLS

## 1. NOW-shaped `ADOPTED`

```text
repair owner exists
canonical DB position arrives
adoption returns ADOPTED
```

Expect:

```text
adoption called once
add_position = 0
exactly one active owner
owner id = canonical UUID
no broker-repair owner
client/mode/OCC unchanged
```

---

## 2. `ALREADY_CANONICAL_REPAIR_REMOVED`

Setup canonical owner + stale matching repair object.

Expect:

```text
add_position = 0
one canonical owner remains
repair removed
```

---

## 3. `NO_REPAIR_FOUND`

No repair owner exists.

Expect:

```text
adoption called first
NO_REPAIR_FOUND
existing reconciler canonical seed runs exactly once
postcondition proves one canonical owner
```

---

## 4. `RETRY_IDENTITY_CONFLICT`

Synthetic candidate conflicts with canonical identity.

Expect:

```text
add_position = 0
no owner count increase
no broker calls
critical diagnostic
```

---

## 5. generic `RETRY_*`

Parameterize multiple retry dispositions if the API exposes them.

Every retry outcome must block generic add.

---

## 6. repeated reconciliation is idempotent

Run the same position through multiple reconciler polls.

Expected:

```text
poll 1: ADOPTED or safe canonical seed
poll 2+: canonical state remains one owner
no duplicate ManagedPosition
no recreated broker-repair owner
```

---

## 7. canonical owner already present, no repair

Expected:

```text
no duplicate behavior-active owner after repeated seed
```

If adoption returns `NO_REPAIR_FOUND`, generic add must still satisfy the exact one-owner postcondition.

---

# REQUIRED NEGATIVE CONTROLS

## Cross-client isolation

Repair owner:

```text
other@example.com
same OCC
same mode
```

Canonical Jason position must not adopt/remove/mutate that owner.

---

## LIVE/PAPER isolation

Repair owner PAPER, canonical position LIVE.

Must fail closed or report no matching repair. It must never convert PAPER ownership into LIVE ownership.

Likewise LIVE repair must not donate to PAPER.

---

## Wrong OCC

Same client/mode but different strike/expiry/side.

No adoption.

Ticker-only equality is insufficient.

---

## Unknown/malformed mode

Examples:

```text
None
""
"LIVE" if current canonical normalizer rejects noncanonical raw values
"unknown"
```

Must not create or adopt a behavior-active owner until exact mode is proven under current production semantics.

---

## Missing canonical position id

Blank/null/placeholder canonical identity cannot become adoption authority.

No generic fallback owner should be created merely to keep monitoring alive.

---

## Conflicting durable ENTRY evidence

If canonical position says one broker/local order identity and exact filled ENTRY says another:

```text
HOLD
add_position = 0
adoption = 0 or fail-closed before mutation
```

Do not overwrite identity.

---

## Ambiguous filled ENTRY rows

If implementation needs a filled ENTRY lookup and more than one candidate survives the exact canonical position fence:

```text
HOLD
```

Do not `ORDER BY newest LIMIT 1` away a real ambiguity.

---

## Adoption exception

`adopt_canonical_position_identity()` raises.

Expect:

```text
add_position = 0
no broker submit/cancel
no proof/queue mutation
visible error
```

---

## Postcondition failure after reported success

Mock adoption to return `ADOPTED` but leave:

```text
repair owner still active
or two owners active
or wrong canonical id active
```

Reconciler must report failure.

It must not trust the disposition blindly.

---

# #545 INTEGRATION REQUIREMENTS

#545 is the immutable historical-entry slice.

After rebase, 517-B must preserve all #545 behavior.

Specifically:

```text
missing historical underlying_entry
-> never current quote
-> stays 0.0 / untrusted
```

Canonical adoption is an identity operation, not permission to invent historical price context.

Required regression:

```text
repair owner exists
canonical position underlying_entry is untrusted/0 after #545
adoption succeeds
-> canonical owner still has 0/untrusted historical entry state
-> no current quote call occurs to fabricate it
```

If durable exact ENTRY metadata proves the historical entry through #545's existing path, that proven value may be passed to adoption.

Do not create a second historical-entry lookup implementation in 517-B.

---

# #585 INTEGRATION REQUIREMENTS

#585 supersedes #516 and owns broker-open / DB-lag durable identity creation.

After #585:

```text
new broker-open recovery should prefer canonical filled ENTRY position_id
and should not create a new engine-only broker-repair owner when DB identity is unconfirmed
```

517-B remains necessary for:

- restart state containing a preexisting repair owner;
- an owner created by older deployed code before rollout;
- races where canonical state becomes visible after a repair owner was already installed by an older process;
- any other existing `broker-repair-*` object encountered by the reconciler.

517-B must not duplicate #585's DB INSERT / UUID / broker-position repair logic.

Required regression after #585 ancestry:

```text
normal new #585 canonical recovery
-> reconciler sees canonical position
-> no repair owner exists
-> adoption returns NO_REPAIR_FOUND
-> one canonical owner only
```

---

# #544 INTEGRATION

#544 removed conflicting standing protective broker stops from fill-monitor.

517-B must not reintroduce any broker-side standing exit order or broker stop ownership.

No broker submit/cancel call belongs in this PR.

---

# RELATIONSHIP TO OLD #517

Old #517 attempted to solve both:

```text
A. historical underlying-entry truth
B. canonical owner adoption
```

and expanded into a large package/shim implementation.

The replacement plan is now:

```text
#545 = 517-A
this PR = 517-B
```

Once both replacements are merged and validated, old #517 should be closed as superseded.

Do not cherry-pick old #517 wholesale.

Old #517 may be read only as historical incident context or test inspiration.

---

# STRICT NON-SCOPE

Do not modify:

- scanner logic;
- signal generation;
- watcher logic;
- deferred materializer;
- contract selector;
- score thresholds;
- spread/OI/volume/delta/DTE/moneyness gates;
- affordability;
- capital allocation;
- sizing;
- daily loss controls;
- exit thresholds;
- runner logic;
- stop logic;
- trailing logic;
- quote pricing rules;
- proof taxonomy;
- manual-close reconciliation;
- trade queue semantics;
- broker submit;
- broker cancel;
- Tradier adapter behavior;
- positions schema;
- orders schema.

Do not create a new schema column for ownership convergence.

Do not change `proof_trades`.

Do not change `trade_queue`.

Do not make broker calls from adoption handling.

---

# MUTATION AUDIT

Expected mutation authority for this PR:

| Surface | Allowed |
|---|---|
| Broker submit | NONE |
| Broker cancel | NONE |
| Broker positions read | existing reconciler behavior only |
| Orders | read-only exact ENTRY evidence if required |
| Positions | read-only for adoption decision; existing unrelated reconciler persistence unchanged |
| Exit engine memory | YES — existing canonical adoption API / existing canonical add path |
| proof_trades | NONE |
| trade_queue | NONE |
| signals | NONE |

Any new durable money-path mutation outside this table is a scope breach.

---

# OBSERVABILITY

At minimum emit operator-visible diagnostics for:

```text
RECONCILER_CANONICAL_OWNER_ADOPTED
RECONCILER_CANONICAL_OWNER_ALREADY_CANONICAL
RECONCILER_CANONICAL_OWNER_SEEDED_NO_REPAIR
RECONCILER_CANONICAL_OWNER_RETRY_HOLD
RECONCILER_CANONICAL_OWNER_POSTCONDITION_FAILED
RECONCILER_CANONICAL_OWNER_ADOPTION_EXCEPTION
```

Exact naming may follow existing conventions, but diagnostics must contain:

```text
client_id
execution_mode
contract
canonical position_id
adoption disposition
owner count / owner ids when safe to log
whether generic add ran
```

Do not claim “monitoring retained” unless an exact behavior-active owner has actually been proven.

---

# REQUIRED TEST MATRIX

At minimum run:

```bash
python -m pytest -q \
  tests/test_p0_reconciler_canonical_owner_adoption.py \
  tests/test_p0_filled_entry_durable_identity_handoff.py \
  tests/test_p0_live_executable_bid_pnl.py \
  tests/test_p0_reconciler_historical_entry_truth.py \
  tests/test_p0_reconciler_external_close_broker_truth.py
```

Also run all exact #470 canonical adoption / durable-identity tests available on final main.

Also run #176 degraded-LIVE exit safety tests unchanged.

Also run merged #585 focused tests unchanged.

Then run authoritative exact-head P0 GitHub Actions with PostgreSQL.

No merge based only on local tests.

---

# IMPLEMENTATION SIZE EXPECTATION

Preferred production scope:

```text
ap_reconciler.py
```

Expected test scope:

```text
tests/test_p0_reconciler_canonical_owner_adoption.py
.github/workflows/p0_regression.yml   # only if test registration is required
```

This should not become another 1,000-line production rewrite.

A few focused helpers are acceptable for:

- exact filled ENTRY metadata read;
- adoption outcome routing;
- read-only exact-owner postcondition.

If production code exceeds what those three responsibilities reasonably require, stop and explain the expansion before continuing.

Finding another real bug during implementation does **not** authorize adding it to this PR.

New bug -> separate PR.

---

# COMPLETION REPORT REQUIRED FROM CODEX

Before marking review-ready, post all of the following:

```text
SPEC BASE SHA
IMPLEMENTATION BASE SHA
FINAL HEAD SHA
FINAL CURRENT MAIN SHA
#545 merged SHA
#585 merged SHA
exact changed files
production LOC delta
fail-first test and exact pre-fix failure
exact reconciler call path before/after
exact adoption kwargs source map
ADOPTED behavior
ALREADY_CANONICAL_REPAIR_REMOVED behavior
NO_REPAIR_FOUND behavior
RETRY_* behavior
adoption exception behavior
postcondition behavior
client isolation proof
LIVE/PAPER isolation proof
exact OCC proof
ambiguous evidence proof
#545 compatibility
#585 compatibility
#470 compatibility
#176 compatibility
broker submit call-site delta
broker cancel call-site delta
orders mutation delta
positions mutation delta
proof_trades mutation delta
trade_queue mutation delta
focused local test counts
PostgreSQL test counts
exact-head P0 workflow run id
exact SHA tested by CI
workflow conclusion
git diff main...HEAD --stat
remaining known limitations
final MERGE / HOLD / HARD HOLD recommendation
```

Do not report tests that were skipped as passed.

---

# MERGE GATE

## MERGE only if

- #545 is in ancestry;
- final #585 fix is in ancestry;
- implementation is rebased onto current main;
- only the canonical-owner adoption slice is changed;
- reconciler calls existing canonical adoption API before generic add;
- `ADOPTED` never falls through to add;
- `ALREADY_CANONICAL_REPAIR_REMOVED` never falls through to add;
- `RETRY_*` never falls through to add;
- only `NO_REPAIR_FOUND` may use existing canonical seed;
- postcondition proves exactly one canonical behavior-active owner;
- no matching `broker-repair-*` owner remains after successful adoption;
- cross-client and cross-mode ownership cannot collapse;
- exact OCC fencing remains mandatory;
- #545 historical entry truth remains immutable;
- no current quote becomes historical entry;
- #585 behavior remains intact;
- #470 adoption tests remain green;
- #176 degraded-LIVE protections remain green;
- no broker submit/cancel diff exists;
- no proof or queue mutation is added;
- new focused test is registered in authoritative P0 CI;
- exact-head P0 CI passes;
- the workflow asserts `git rev-parse HEAD` equals the submitted PR head SHA;
- independent final diff audit returns MERGE.

## HOLD if

Implementation is logically correct but waiting on:

- #545 merge;
- #585 merge;
- final rebase;
- exact-head CI;
- independent audit.

## HARD HOLD if

- reconciler still calls generic add before trying adoption;
- any `RETRY_*` outcome can seed a second owner;
- successful adoption can leave both synthetic and canonical owners behavior-active;
- cross-client/mode/OCC adoption is possible;
- adoption errors silently fall through to generic add;
- current quote/current underlying becomes historical entry again;
- broker submit/cancel authority appears;
- exit strategy thresholds change;
- #585 or #545 code is duplicated into this PR;
- old #517 is restored wholesale.

---

# ONE-LINE IMPLEMENTATION TARGET

> When the reconciler sees the canonical DB position, it must let the exit engine **adopt that canonical identity first**, and it may use generic `add_position()` only when the adoption authority explicitly says `NO_REPAIR_FOUND`.
