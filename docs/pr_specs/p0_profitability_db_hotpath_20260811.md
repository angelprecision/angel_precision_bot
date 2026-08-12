# P0 Production DB Hot-Path Latency + Shape Parity

## Status

**IMPLEMENTATION/CI PASS — RELEASE EVIDENCE GATE EXTERNAL.** This commit records the contract implementation and exact-head CI evidence. Merge/deploy authorization remains separate and requires independently attached production EXPLAIN, lock-safety, catalog, and migration-ledger proof.

Base: `main@3f9f4c65b79c80c6d3442a9144a0018d70624fb6`.

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

### Current-main inventory completed

The exact runtime callers are:

- `APOrderMonitor._check_armed_retries` — canceled `ENTRY` rows with
  `meta->>'retry_status' = 'ARMED'`, ordered by `updated_ts`, `LIMIT 64`;
- `APOrderMonitor._recover_stale_inflight_retries` — canceled `ENTRY` rows
  with `meta->>'retry_status' IN ('IN_FLIGHT','SUBMITTING')`, ordered by
  `updated_ts`, `LIMIT 16`;
- `ap.preopen_readiness._query_client_state` — active startup-readiness
  `PENDING_TRIGGER` `ENTRY` rows with no broker/submitted/fill identity,
  ordered by `created_ts`;
- `ap.morning_handoff._has_unowned_pending_trigger_orders` — watcher reseed
  guard using the same exact `PENDING_TRIGGER` identity predicates.
- `APStartupRecovery._recover_deferred_breach_lifecycles` — continuously
  revisited by `ClientRunner`'s approximately 20-second health loop; it keeps
  legacy `LOWER(TRIM(COALESCE(execution_mode, '')))` and
  `UPPER(COALESCE(status, ''))` normalization, accepts null/blank broker
  identity, requires `submitted_ts IS NULL`, and orders by `created_ts`.

The retry callers already parse `retry_ready_at` and attempt counters in
Python. The implementation therefore adds only four narrow partial indexes in
`migrations/20260811_orders_retry_hotpath_indexes.sql`; the fourth is an
expression/partial index matching the deferred-recovery mode/status
normalization exactly. It does not cast JSON metadata or rewrite client/mode
ownership predicates, and it does not change `ap_recovery.py` runtime logic.

Focused contract coverage is in
`tests/test_p0_profitability_db_hotpath.py` and runs through
`.github/workflows/p0_db_hotpath.yml`. It includes exact result-set comparison
before/after indexes for all four exact paths, clean PostgreSQL idempotency with
actual index-definition/expression attestation, direct fixture
EXPLAIN/index-eligibility evidence for ARMED, stale retry, both
PENDING_TRIGGER paths, recursive scan-work bounds beneath `Limit`, the
PENDING_TRIGGER residual `filled_ts IS NULL` filter, malformed-metadata guards,
and the two known production schema-shape checks across root and nested
production files. The focused workflow path filters mirror those root and
nested production globs, so a future schema-shape regression cannot bypass
this contract by changing an unlisted production module.

### 1. Inventory slow queries from production logs

Capture the exact current-main SQL and call sites for at least:

- canceled ENTRY + `retry_status=ARMED`
- canceled ENTRY + `retry_status IN (IN_FLIGHT,SUBMITTING)`
- PENDING_TRIGGER recovery scans
- deferred-breach `PENDING_TRIGGER` recovery scans from `APStartupRecovery`
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
- `(client_id, lower(trim(coalesce(execution_mode,''))), created_ts)` expression/partial for the exact deferred-breach recovery predicate, including normalized status and null/blank broker identity

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
3. Existing PENDING_TRIGGER recovery returns same exact rows.
4. Deferred-breach PENDING_TRIGGER recovery returns same exact rows, including legacy mode/status and blank broker normalization.
5. LIVE/PAPER rows remain isolated.
6. null execution mode does not default to LIVE.
7. legacy valid metadata remains classified consistently.
8. malformed retry timestamp behavior unchanged.
9. negative retry timestamp/counter behavior unchanged.
10. duplicate rows/order identities unchanged.
11. ordering by readiness/update/create time preserved.
12. LIMIT semantics preserved.
13. no broker calls caused by preflight/index tests.
14. migration idempotent.
15. clean PostgreSQL applies migration twice safely and attests actual definitions.
16. EXPLAIN directly proves ARMED, stale retry, existing PENDING_TRIGGER, and deferred-recovery indexes are eligible/used.
17. recursive plan inspection proves underlying scan work is bounded beneath `Limit`.
18. the existing PENDING_TRIGGER index's `filled_ts IS NULL` residual filter is visible and selective on the fixture.
19. schema-shape tests reject `trade_queue.ticker` and absent `meta` assumptions across root and nested production files.
20. no client-id rewrite.
21. retry execution-mode fences are not rewritten, while deferred recovery retains its explicit legacy normalization.

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

The active ENTRY lifecycle no longer spends 10-15 seconds scanning thousands of irrelevant rows for small retry/recovery result sets, including the continuously polled deferred-breach path, and production-shape mistakes are caught before runtime.

## Release verdict

Current state: **IMPLEMENTATION/CI PASS; RELEASE PROOF AND LEDGER RECONCILIATION
REMAIN EXTERNAL GATES.** Do not treat fixture planner eligibility, GitHub
mergeability, or PR prose as deployed-production proof. Record production
`EXPLAIN (ANALYZE, BUFFERS)`, lock-window safety, exact catalog definitions,
`indisvalid`/`indisready`, and the sanctioned migration-ledger checksum before
merge/deploy authorization.

Implementation requires production-safe EXPLAIN evidence, migration review,
exact-head P0 CI, and independent money-path review.
