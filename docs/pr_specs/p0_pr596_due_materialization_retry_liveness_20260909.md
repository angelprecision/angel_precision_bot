# P0 #596 — Due Materialization Retry Liveness

**Status: DRAFT / HARD HOLD / NO MERGE / NO DEPLOY**

**Base:** `main@d404df34e00522ba2cce995b6a0129ba39b83944`

## Objective

Restore one specific live failure without introducing another lifecycle architecture:

```text
PENDING_TRIGGER
+ exact durable identity
+ confirmed trigger
+ canonical RETRY_PENDING
+ retry time due
+ no broker handoff
-> existing retry path executes exactly once
```

This is an execution-liveness fix. It is **not** a signal-quality change and must not loosen selector, spread, OI, volume, delta, DTE, moneyness, scoring, sizing, capacity, or risk policy.

## What the current main already has

The clean current main already contains:

- canonical `WAITING_RETRYABLE` classification;
- canonical `materialization_status=RETRY_PENDING` fields;
- existing `RETRY_WAIT` watcher lifecycle;
- existing OSM retry ownership/adoption primitives;
- canonical selector retry taxonomy.

Therefore **do not transplant old PR #596 wholesale**. The old branch contains unrelated #601-era files and a large phase-one recovery design that is not required for this defect.

## Confirmed baseline blocker

`APOrderMonitor._get_active_entry_orders()` on current main does not project durable `client_id` (and `kind`) even though `_canonical_pending_trigger_rearm()` passes the projected row into `PendingTriggerRestartRecovery`.

Recovery requires the durable client identity and intentionally fails closed when it is absent. This produces the observed live failure:

```text
RESTART_RECOVERY_MISSING_DURABLE_CLIENT_ID
-> UNRESOLVED
-> PENDING_TRIGGER remains unresolved
-> due RETRY_PENDING is never consumed
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

## Due-retry handoff

After the projection fix, prove the real production caller path:

```text
APOrderMonitor
  -> PendingTriggerRestartRecovery
  -> classify_pending_trigger_row()
  -> WAITING_RETRYABLE
  -> existing canonical retry ownership/adoption
  -> existing RETRY_WAIT watcher path
  -> existing selector/materialization path
```

If the existing watcher/adoption path consumes a **due** retry correctly after durable identity is present, make **no additional production change**.

If it does not, add exactly one narrow handoff at the existing seam so a due canonical retry is adopted by the existing `RETRY_WAIT` watcher path. Do not create a second retry worker, scheduler, broker authority, or state machine.

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

1. **MO production-shaped replay**
   - exact durable client identity present;
   - confirmed trigger;
   - canonical `RETRY_PENDING`;
   - due `materialization_next_retry_at`;
   - no broker handoff;
   - existing retry path executes once.

2. **MMM production-shaped replay**
   - same proof as MO.

3. **WFC positive control**
   - existing successful deferred retry behavior remains unchanged.

4. **Not-due control**
   - future retry remains waiting; no selector call.

5. **Concurrent recovery**
   - two workers cannot both own/execute the same retry.

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

**HARD HOLD until the live caller path is behaviorally proven.**

A green unit test that mocks away the order-monitor row shape is insufficient.

The target is deliberately boring:

```text
one durable identity projection fix
+
possibly one existing-path handoff fix if the real replay proves it necessary
+
focused tests
=
closed liveness hole
```

Do not make the retry system clever. Clever retry systems are how databases acquire folklore.
