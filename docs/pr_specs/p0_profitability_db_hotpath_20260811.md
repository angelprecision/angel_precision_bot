# P0 Production DB Hot-Path Latency + Shape Parity

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This PR is infrastructure, not strategy. It exists because profitable entry intelligence is useless if the order/watcher/retry path routinely waits 10-15 seconds on PostgreSQL or times out.

## Confirmed production evidence — 2026-08-11

Supabase PostgreSQL logs show repeated statement timeouts and slow ENTRY lifecycle queries.

Examples observed in production:

### Jason LIVE canceled-entry stale retry recovery

A query selecting canceled ENTRY rows for `jasoncosby1@gmail.com` with `meta->>'retry_status' IN ('IN_FLIGHT','SUBMITTING')`, execution-mode predicates, `ORDER BY updated_ts`, and `LIMIT 16` logged approximately **15.27 seconds**.

The plan used `idx_orders_client_status`, retrieved thousands of candidate rows by client/status, then heap-filtered on kind, JSONB retry status, timestamps, execution mode, and metadata before sorting.

### Tradefluence PAPER armed retry scan

Canceled ENTRY rows with `meta->>'retry_status'='ARMED'` and numeric `retry_ready_at` filtering logged approximately **14.18 seconds**.

### Jose PAPER armed retry scan

Similar query logged approximately **10.73 seconds**.

### Jose pending-trigger recovery scan

PENDING_TRIGGER ENTRY rows with mode, broker-id/submitted guards, and created-time ordering logged approximately **10.14 seconds**.

Production also shows repeated `canceling statement due to statement timeout` errors.

Separately, diagnostic/analytics code is issuing queries against production shapes that do not exist, including `trade_queue.ticker` and generic `meta` assumptions on tables without that column. Those failures add noise/load and prove schema-shape drift.

## Objective

Make the active ENTRY/retry/watcher ownership queries predictably fast and make production-shape mismatches fail at preflight/test time instead of repeatedly hitting PostgreSQL in production.

This PR must **not** change strategy semantics, retry limits, order ownership, broker behavior, or eligibility.

## Required implementation workflow

### 1. Inventory slow queries from production logs

Capture the exact current-main SQL and call sites for at least:

- canceled ENTRY + `retry_status=ARMED`
- canceled ENTRY + `retry_status IN (IN_FLIGHT,SUBMITTING)`
- PENDING_TRIGGER recovery scans
- any watcher/restart query appearing repeatedly above the statement-timeout threshold

For each, record:

- caller
- frequency
- client/mode scoping
- filters
- ORDER BY/LIMIT
- current index used
- rows scanned vs rows returned
- production `EXPLAIN (ANALYZE, BUFFERS)` where safe/read-only

Do not optimize a reconstructed query that differs from runtime SQL.

### 2. Add minimal indexes matching real predicates

Prefer narrow partial/expression indexes that support exact hot-path predicates without creating huge write amplification.

Candidate forms to evaluate, not blindly copy:

- `(client_id, updated_ts)` partial where `kind='ENTRY' AND status='CANCELED'`
- expression/partial index including `(meta->>'retry_status')` for canceled-entry retry scans
- expression index for numeric/validated `retry_ready_at` only if malformed legacy JSON cannot make index creation/queries unsafe
- `(client_id, execution_mode, created_ts)` partial where `kind='ENTRY' AND status='PENDING_TRIGGER' AND broker_order_id IS NULL AND submitted_ts IS NULL`

The implementation must use real production cardinality/EXPLAIN evidence to choose indexes.

Do not add one giant catch-all index containing every JSON field.

### 3. Preserve malformed-metadata semantics

JSONB expression indexes and casts can break on malformed values.

Current retry policy includes fail-closed parsing semantics for malformed/negative counters/timestamps.

Any index/query rewrite must preserve:

- malformed retry metadata classification
- blank/null handling
- legacy field compatibility
- exact mode semantics
- no implicit LIVE default

Do not change behavior merely to make an index usable.

### 4. Remove avoidable function-wrapped predicates where safe

Queries such as `LOWER(TRIM(COALESCE(execution_mode,'')))='paper'` can prevent ordinary index use.

If execution mode is canonically normalized on current main, use exact indexed predicates.

If legacy rows still require normalization, do not silently exclude them. Either:

- use a reviewed expression index matching the exact predicate; or
- separate modern fast path from bounded legacy recovery path with explicit diagnostics.

No live/paper taxonomy drift is allowed for performance.

### 5. Add schema-shape tests/preflight

Known bad production assumptions must be caught before deployment.

At minimum prove:

- `trade_queue.ticker` is not assumed when ticker is payload/related-signal data.
- modules querying a `meta` column use it only on tables that actually have it.
- required intelligence/counterfactual tables are separately attested by the intelligence-truth PR.

A developer/operator diagnostics query must not spam production with repeated undefined-column errors.

### 6. Bound retry scan work

The query should use an index that returns approximately the rows relevant to the LIMIT rather than fetching thousands and filtering/sorting in heap.

Acceptance target should be based on production EXPLAIN evidence. Initial target:

- hot-path scans should be comfortably sub-second under current table size;
- preferably tens of milliseconds for indexed LIMIT queries;
- no 10+ second scans for routine order-monitor ticks.

Do not claim performance from unit tests alone.

## Required tests

1. ARMED retry query returns same exact rows before/after index/query optimization.
2. IN_FLIGHT/SUBMITTING stale recovery returns same exact rows.
3. PENDING_TRIGGER recovery returns same exact rows.
4. LIVE/PAPER rows remain isolated.
5. null execution mode does not default to LIVE.
6. legacy valid metadata remains classified consistently.
7. malformed retry timestamp behavior unchanged.
8. negative retry timestamp/counter behavior unchanged.
9. duplicate rows/order identities unchanged.
10. ordering by readiness/update/create time preserved.
11. LIMIT semantics preserved.
12. no broker calls caused by preflight/index tests.
13. migration idempotent.
14. clean PostgreSQL applies migration twice safely.
15. EXPLAIN proves intended index is eligible/used on production-shaped fixture.
16. production-shaped table scale test demonstrates bounded runtime.
17. schema-shape test rejects `trade_queue.ticker` assumption.
18. schema-shape test rejects querying absent `meta` column.
19. no client-id rewrite.
20. no execution-mode rewrite.

## Scope budget

Expected:

- one idempotent index migration
- the exact query-owner modules only if SQL must change
- schema/preflight test/module if necessary
- focused tests + P0 workflow

No strategy/intelligence scoring changes.

## Explicit non-goals

Do not change:

- retry max attempts
- retry delays
- watcher lifetime
- trigger policy
- selector budgets
- scanner thresholds
- intelligence score
- broker submit/cancel
- order-state semantics
- position state
- proof trades
- exits

## Money-path safety

This PR changes database access performance, not business decisions.

Required final assertions:

- broker submit/cancel functions changed: **NO**
- order state transition semantics changed: **NO**
- position mutations changed: **NO**
- proof mutations changed: **NO**
- queue eligibility changed: **NO**
- `client_id` preserved: **YES**
- `execution_mode` preserved: **YES**
- PAPER/LIVE taxonomy preserved: **YES**

## Definition of done

The active ENTRY lifecycle no longer spends 10-15 seconds scanning thousands of irrelevant rows for a LIMIT-16 result, and production-shape mistakes are caught before runtime.

## Release verdict

Current state: **HARD HOLD — docs only.**

Implementation requires production-safe EXPLAIN evidence, migration review, exact-head P0 CI, and independent money-path review.