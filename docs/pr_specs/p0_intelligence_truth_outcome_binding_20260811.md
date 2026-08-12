# P0 Intelligence Truth + Canonical Outcome Binding

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is the first PR in the profitability-intelligence repair stack. It does **not** change entry eligibility, scanner thresholds, broker behavior, watcher behavior, selector behavior, position sizing, exits, or proof-trade economics. It repairs the evidence plane so later intelligence work is trained and evaluated against truthful production data.

## Why this is first

Angel Precision cannot improve trade selection if intelligence decisions cannot be joined deterministically to actual outcomes.

Current production evidence on 2026-08-11 shows the learning/analytics plane is not trustworthy:

- PostgreSQL logs show `relation "trade_performance" does not exist` while `ap/performance_tracker.py` attempts to insert into `trade_performance`.
- PostgreSQL logs show repeated `relation "public.blocked_signal_counterfactuals" does not exist` while `ap/counterfactual_tracker.py` and block paths attempt to use that table.
- PostgreSQL logs also show analytics/debug queries using columns that do not exist in real production shapes, including `trade_queue.ticker` and generic `meta` assumptions.
- The legacy intelligence audit outcome path does not use canonical economic identity. It can attempt to associate outcome data with a decision using ticker/timestamp-style matching, which is not sufficient for multiple opportunities in the same ticker and cannot be relied on across decision time vs close time.
- A result that cannot be tied back to the exact pre-entry decision must not influence profitability tuning, promotion, ranking, or claims about filtered-vs-unfiltered performance.

The invariant for this PR is simple:

> **No intelligence system may learn from, rank from, or report on an outcome unless the originating pre-entry decision and the terminal economic result are bound by exact durable identity.**

## Canonical economic identity

Every intelligence observation that is intended to be evaluated against a later trade outcome must preserve, when available:

- `client_id`
- `execution_mode`
- `signal_id`
- `canonical_signal_id`
- `local_order_id`
- broker order identity when one exists
- position identity when one exists
- selected OCC contract when one exists
- intelligence phase (`PRETRIGGER`, `PREOPEN`, later `BREACH`, later `CONTRACT_SELECTED`)
- intelligence policy/profile version
- configuration hash
- git commit
- immutable decision timestamp / data-as-of timestamp

### Required join priority

For an executed trade, the preferred binding is:

1. exact client
2. exact execution mode
3. exact originating ENTRY `local_order_id`
4. exact position / canonical entry lineage
5. exact canonical signal identity as supporting evidence

`signal_id` alone is not enough if legacy/recovery paths can reuse or transform identity.

Ticker is never a unique key.

Timestamp is never a unique key.

Ticker + timestamp is never a canonical economic identity.

Contract symbol alone is never a canonical economic identity because the same OCC contract can be traded more than once.

## Required implementation strategy

### 1. Inventory every intelligence/performance writer and reader

Before changing code, trace current-main production callers for:

- `ap/performance_tracker.py`
- `intelligence_bridge.record_trade_outcome`
- `ap_intelligence/ap_audit_log.py`
- `ap/counterfactual_tracker.py`
- `ap_intelligence_snapshots`
- `decision_events`
- `proof_trades`
- any ranking/performance endpoint or report that consumes these sources

Produce an explicit table in the implementation PR description:

| Producer | Durable table/file | Identity fields written | Outcome-capable? | LIVE/PAPER isolation | Action |
|---|---|---|---|---|---|

No writer may remain silently best-effort if downstream code assumes its data exists.

### 2. Choose one canonical executed-outcome source

Do **not** create a third competing truth table if `proof_trades` already contains the canonical terminal economic result and required identity after the active proof stack is stabilized.

Preferred architecture:

- `proof_trades` remains terminal trade-result authority.
- intelligence observation tables/snapshots remain pre-entry evidence authority.
- a dedicated immutable binding/measurement table may reference both identities if necessary, but must not duplicate or reinterpret terminal P&L.

If current production `proof_trades` cannot yet safely support exact binding because of known duplicate-proof issues, the implementation must HOLD and explicitly depend on the proof-truth PR rather than inventing a fuzzy fallback.

### 3. Eliminate or repair the nonexistent `trade_performance` dependency

Current code attempts to insert into a table that production logs prove does not exist.

Implementation must choose one of these paths and justify it:

A. remove the production dependency and derive evaluation from canonical terminal proof truth; or

B. add an idempotent migration plus schema attestation and prove the table has a unique economic identity and no competing terminal authority.

Default preference: **A**, unless a separate performance table is proven necessary.

No silent `except Exception: log.debug(...)` may make a missing production table look like a healthy learning loop.

### 4. Deploy and attest `blocked_signal_counterfactuals` correctly

Repository contains `migrations/20260704_blocked_signal_counterfactuals.sql`, but production logs prove the relation is absent while runtime attempts to insert/query it.

Required:

- inspect current migration content against real production schema assumptions;
- confirm exact required columns and indexes;
- include the table in schema attestation if runtime depends on it;
- apply in disposable PostgreSQL integration tests;
- prove startup/preflight detects absence rather than silently claiming counterfactual learning is operational;
- preserve `client_id` + `execution_mode` isolation;
- ensure PAPER counterfactuals can never become LIVE_OFFICIAL outcomes.

If the tracker remains best-effort by design, its health must still be observable and must never be reported as populated when writes are failing.

### 5. Replace fuzzy outcome matching with exact durable binding

The legacy audit updater must not search by ticker plus exact timestamp or any equivalent fuzzy shape.

Required interface should conceptually look like:

`record_intelligence_outcome(client_id, execution_mode, canonical_signal_id, originating_local_order_id, position_id, proof_trade_id, realized_pnl, return_pct)`

The exact implementation can differ, but all available identity must be transported.

For old rows missing exact identity:

- classify as `UNBOUND_LEGACY` / not training eligible;
- never guess by ticker/time proximity;
- never upgrade PAPER to LIVE from client name, environment, broker URL, or missing mode;
- never bind when two candidate outcomes exist.

### 6. Make outcome ingestion idempotent

The same terminal fill/proof may be observed by OSM, position manager, reconciler, or restart recovery.

Repeated callbacks must produce one measurement for one economic trade.

Concurrency test required: two writers racing to bind the same terminal outcome -> one durable binding / one measurement.

### 7. Preserve raw evidence

The measurement record must preserve pointers/hashes to the exact intelligence snapshot/profile that existed **before entry**.

Never recompute historical features using future candles and call them pre-entry evidence.

Never overwrite a pre-entry snapshot after outcome is known.

Outcome fields must be separate from feature payloads to prevent leakage.

## Production-shape requirements

Tests must use the real current schema shape, not invented convenience columns.

Specific known hazards to test:

- `trade_queue` does not have a top-level `ticker` column in production; ticker may live in payload/related signal data.
- tables do not all share a generic `meta` column.
- execution mode can appear in legacy rows with inconsistent casing/secondary fields; resolution must fail closed on conflict.
- `proof_trades` may contain historical repair rows that are not training eligible.
- null/blank `local_order_id` legacy rows cannot be guessed into official training identity.

## Required tests

Minimum acceptance matrix:

1. exact LIVE client + mode + entry local order + canonical signal -> exact LIVE proof binds once.
2. exact PAPER trade binds only to PAPER measurement.
3. same ticker, two different trades same day -> each binds to its own outcome.
4. same OCC contract reused twice -> no cross-binding.
5. same signal family across two clients -> no cross-client binding.
6. missing mode -> unbound, no fallback to environment.
7. conflicting LIVE/PAPER evidence -> quarantine/unbound.
8. missing local order but exact unique position lineage -> only bind if current canonical lifecycle proves it is safe.
9. two candidate proofs -> ambiguity, zero binding.
10. duplicate callback -> one measurement.
11. concurrent duplicate callbacks -> one measurement.
12. counterfactual blocked setup never enters executed-trade metrics.
13. PAPER/counterfactual rows never enter LIVE profitability metrics.
14. missing `blocked_signal_counterfactuals` migration -> explicit preflight/schema failure.
15. missing required executed-outcome schema -> explicit health failure, not silent success.
16. outcome arrives before intelligence snapshot persistence -> bounded reconciliation later, never lossy guess.
17. intelligence snapshot exists but trade never broker-submits -> not an executed outcome.
18. submitted but never filled -> not a winning/losing terminal trade.
19. partial fills and later final close -> exactly one economic terminal measurement under current proof semantics.
20. replay of historical legacy row without exact identity -> `UNBOUND_LEGACY`, excluded from training.

## Observability required

Add/retain structured counts for:

- intelligence observations captured
- executed outcomes available
- outcomes bound exactly
- outcomes unbound due to missing identity
- outcomes quarantined due to conflict/ambiguity
- counterfactual inserts succeeded/failed
- schema/preflight health
- LIVE training-eligible sample count
- PAPER research sample count

A dashboard/report must never claim “N trades learned from” when N includes unbound, PAPER, duplicated, or counterfactual rows.

## Scope budget

Expected production scope, current-main dependent:

1. `ap/performance_tracker.py` or its removal/replacement seam
2. `intelligence_bridge.py` outcome-record interface only if still used
3. `ap/counterfactual_tracker.py`
4. one canonical intelligence/outcome binding module if necessary
5. `ap/schema_attestation.py`
6. one idempotent migration only where production schema is genuinely missing
7. focused tests + P0 workflow

Do not touch:

- scanners
- score thresholds
- Gate G admission semantics
- watcher trigger logic
- contract selector ranking
- broker submit/cancel
- OSM lifecycle semantics
- position sizing
- exits
- profit targets/stops
- queue eligibility

If implementation requires these, stop and split the work.

## Money-path safety

This PR must prove:

- broker submit calls introduced: **0**
- broker cancel calls introduced: **0**
- order lifecycle mutation changes: **0** except optional immutable analytics pointers if explicitly justified
- position quantity/state mutation changes: **0**
- `proof_trades` economic mutation changes: **0**; canonical proof truth is consumed, not rewritten
- `trade_queue` execution mutation changes: **0**
- scanner/selector/intelligence admission behavior change: **0**
- `client_id` preservation: exact
- `execution_mode` preservation: exact/fail closed

## Definition of done

This PR is complete only when we can answer, for every training-eligible LIVE trade:

> Which exact pre-entry intelligence observation led to this exact economic trade, and what exact canonical terminal outcome did that trade produce?

with a deterministic database join and no ticker/time guessing.

## Release verdict

Current state: **HARD HOLD — docs only.**

Implementation may move to MERGE consideration only after production-shape PostgreSQL tests, exact-head P0 CI, migration/schema attestation, and an independent review verify the invariants above.