# PR #602 — Order-monitor durable retry projection correction

**Status: DRAFT / HARD HOLD / NO MERGE / NO DEPLOY**

**Base:** `main@d404df34e00522ba2cce995b6a0129ba39b83944`

## Objective

Correct one order-monitor projection defect without claiming ownership of the
startup due-retry executor:

```text
PENDING_TRIGGER
+ exact durable identity
+ canonical RETRY_PENDING
-> APOrderMonitor classifies the row as retry-owned
```

The production change is an order-monitor durable projection correction. It is
not the causal fix for startup due-retry execution and must not add a second
executor, call `resume_deferred_materialization_retry()` from the monitor, or
loosen selector, spread, OI, volume, delta, DTE, moneyness, scoring, sizing,
capacity, or risk policy.

## What the current main already has

The clean current main already contains:

- canonical `WAITING_RETRYABLE` classification;
- canonical `materialization_status=RETRY_PENDING` fields;
- existing `RETRY_WAIT` watcher lifecycle;
- existing OSM retry ownership/adoption primitives;
- canonical selector retry taxonomy.

Therefore **do not transplant old PR #596 wholesale**. The old branch contains unrelated #601-era files and a large phase-one recovery design that is not required for this defect.

## Confirmed order-monitor projection defect

`APOrderMonitor._get_active_entry_orders()` on current main does not project durable `client_id` (and `kind`) even though `_canonical_pending_trigger_rearm()` passes the projected row into `PendingTriggerRestartRecovery`.

Recovery requires the durable client identity and intentionally fails closed
when it is absent. The monitor therefore produced this classification failure:

```text
RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID
-> UNRESOLVED
-> monitor cannot classify the row as retry-owned
```

The first implementation change is therefore the smallest possible projection fix:

```sql
SELECT local_order_id,
       client_id,
       kind,
       broker_order_id,
       ...
FROM orders
...
```

Use the database row as authority. Never synthesize identity from runner context.

## Causal ownership and differential replay

The actual due-retry executor is already a separate startup/runtime consumer:

```text
APStartupRecovery._recover_deferred_breach_lifecycles()
  -> due schedule and durable identity/evidence gates
  -> resume_deferred_materialization_retry()
  -> selector/materializer/OSM handoff
```

The production-shaped PostgreSQL replay seeds the September failure shape:
`PENDING_TRIGGER`, `RETRY_WAIT/RETRY_PENDING`, due `next_retry_at`, exact
client/mode/generation/attempt metadata, and no broker submission evidence. It
proves LIVE MO and PAPER MMM through the real due executor, including two
competing startup consumers: one durable winner, one selector/materializer
handoff, and a losing consumer with zero broker/cancel/proof/position mutation.

The identical replay passes on committed current `main@d404df34e00522ba2cce995b6a0129ba39b83944`, without PR #602's production change. Therefore #602 is **not** the causal fix for live retry-executor liveness. No second handoff is justified. The PR remains a narrowly scoped monitor-projection correctness change, pending independent production consequence and review.

## Explicit non-goals

Do NOT add:

- new retry state machines;
- new broker submit/cancel authority;
- new broker order polling;
- new broker tag snapshots;
- new phase-one recovery logic;
- new ownership concepts;
- new durable columns;
- new watcher lifecycle states;
- new selector calls outside the existing materialization owner;
- new fallback identity sources;
- new retry counters or parallel counter aliases;
- changes to retry limits solely to increase trade flow;
- changes to selector quality gates;
- changes to scanner, scoring, sizing, capacity, or risk policy.

The implementation should be **smaller than old #596**, not a cleaner version of the same architecture.

## Required behavioral proof

1. **Order-monitor projection**
   - PostgreSQL `client_id` and `kind` are projected from the durable row;
   - the fixed row reaches canonical `RETRY_OWNED` classification;
   - the pre-fix row shape remains `UNRESOLVED` with zero mutation.

2. **Startup executor differential replay**
   - LIVE MO and PAPER MMM pass through the actual due executor on both PR #602 and committed `main`;
   - selector/materializer/OSM handoff occurs once under competing-worker CAS;
   - the loser performs zero broker/cancel/proof/position mutation.

3. **WFC positive control**
   - existing successful deferred retry behavior remains unchanged.

4. **Not-due control**
   - future retry remains waiting; no selector call.

5. **Negative controls at the recovery boundary**
   - malformed generation, client mismatch, mode mismatch, future schedule,
     broker/submission evidence, and stale-generation loss never reach selector
     or broker mutation.

6. **Broker ambiguity**
   - any broker intent/order evidence remains HOLD; never retry blindly.

7. **Identity failure**
   - missing/mismatched durable identity remains HOLD/UNRESOLVED; never synthesize it.

8. **Selector quality**
   - all existing selector gates remain byte-for-byte/policy-equivalent.

## Acceptance boundary

The final PR must have:

- only the minimum production files required by the proven failing path;
- focused tests for the actual defect;
- existing P0 inventory coverage retained;
- exact-head P0 green on the implementation SHA;
- merge-ref P0 green using the same valid inventory;
- independent backwards audit;
- no unrelated file churn.

## Merge rule

**HARD HOLD / NO MERGE / NO DEPLOY.** The live caller path is proven on both
PR #602 and current main, so it does not establish #602 as its causal fix.
Keep the PR DRAFT until the independent order-monitor projection consequence
is accepted, or close/narrow it further if that consequence is not material.

A green unit test or `MERGEABLE/CLEAN` status does not authorize merge or
deployment.

The target is deliberately boring:

```text
one honest order-monitor identity/kind projection fix
+ focused projection and differential-boundary tests
=
no claim of causal executor-liveness repair
```

Do not make the retry system clever. Clever retry systems are how databases acquire folklore.
