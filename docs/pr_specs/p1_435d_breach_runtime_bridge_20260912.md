# P1 435-D — Observe-Only BREACH Runtime Bridge

## Status
DRAFT / HARD HOLD. Implementation is present for review only. Do not merge or deploy.

## Purpose
Use the current-main confirmed-trigger seam in `APExecutionCore._on_entry_trigger()`.
Do not create a second execution path or resurrect the historical #330 runtime
implementation.

Canonical path:

`existing confirmed BREACH -> freeze immutable identity/evidence -> bounded nonblocking sideband -> existing selector/submission path`

The sideband worker consumes the frozen artifact through the existing owners:

`#615 freeze -> #625 canonical identity/assembly -> #621 adapter -> #327 job/snapshot`

The established execution callback remains the primary path. The bridge never
calls the selector and never waits for the sideband result.

## Confirmed current-main PIT producer audit

The exact #614 PIT producer is **NOT PRESENT at the confirmed-BREACH runtime
seam** on this current main. The verified production path is:

`ap.queue._dispatch()` -> `enqueue_pretrigger_context_best_effort()` ->
`ap.intelligence_context_handoff.submit_intelligence_enqueue()` -> later
`process_due_intelligence_jobs_once()` -> `build_snapshot_kwargs()` ->
`build_intelligence_context_payload()` ->
`collect_point_in_time_context()`.

The collector is the production #614 implementation, but it is called by the
background materializer (`ap/intelligence_context_materializer.py`,
`build_intelligence_context_payload`, currently line 91), not by the watcher
or the execution callback. The pretrigger job carries a copied signal and is
not proven to be attached back to the callback payload before the trigger.

The runtime continuation is separately:

`ap.queue._dispatch()` -> `entry_watcher.watch()` ->
`WatchedSignal(signal)` -> `APExecutionCore._on_entry_trigger()`.

`watch()` currently copies identity, trigger, and plan metadata (which may
carry caller-provided mappings) into `signal_dict`, along with the durable
first-breach timestamp. It has no code that calls #614 or creates/attaches an
authoritative PIT envelope. `WatchedSignal` retains that signal and parses the
trigger timestamp, but does not call the #614 collector. Therefore #622 must
not claim an authoritative production PIT writer before its seam. If the
watcher/plan does not already contain exact evidence, the bridge emits an
explicit `MISSING`/non-authoritative diagnostic and continues; it adds no
synchronous lookup or network/history work. A future wiring PR must establish
the producer-to-watcher attachment and prove the caller path before changing
this conclusion.

## Hydration ordering proof

The bridge remains before `_refresh_hydrated_prebreach_plan()` and before the
existing selector continuation. That ordering is safe for the current
implementation because hydration changes only the executable contract fields
(`contract_symbol`, `limit_price`, `contracts`, `max_position_usd`, reserved
cost, and contract-selection metadata) and the corresponding contract fields
on `sig`. It does not change client, mode, signal/canonical identity, local
order, materialization generation, trigger timestamp, or PIT/evidence/parent
fields. The focused hydration test snapshots those identity/evidence fields
before and after the real helper. If hydration later expands into any #622 or
#625 identity/evidence field, the bridge must move after hydration without
adding a wait.

## Critical latency invariant
The bridge may not synchronously fetch market history or wait on Tradier, option chain, external APIs, futures, sleeps, joins, or background results before selector execution.

If the exact #614 evidence is not already attached to the watcher/plan, record
an explicit `UNKNOWN/MISSING` diagnostic artifact and continue the existing
trade path unchanged. #615 runs only in the bounded background handoff.

If the current positive materialization generation cannot be proven, keep the
bridge diagnostic-only and do not hand off an identity-less sideband artifact.

A cache miss must never become a selector delay.

## Normalized structure consumer (read-side)

The bridge also exposes the pure, provider-neutral
`resolve_breach_structure_view(frozen_context, expected_identity=...)` helper.
It accepts only an already-produced/frozen mapping and returns a defensive,
read-only view containing `status`, the exact normalized `as_of` and
`decision_boundary`, the complete identity, source versions, grouped
provenance, normalized zones, lower-timeframe acceptance/rejection/reclaim and
penetration facts, and diagnostics. It performs no provider/history/network
read, database wait, persistence, selector call, or eligibility evaluation.

The status taxonomy is deliberately explicit:

- `AUTHORITATIVE`, including `zones=[]` when an authoritative source proves no
  relevant zone exists;
- `STALE` for stale snapshots or stale generations;
- `UNKNOWN` for unavailable or unproven evidence; and
- `INVALID` for malformed or contradictory frozen input.

The helper never fabricates zones or infers volume imbalance. Restarted,
uninterrupted, and materialized/deferred paths are required to produce the
same view from the same frozen context.

## Authority
- observe_only=true
- affected_eligibility=false
- no WAIT/REARM/TERMINAL authority
- no watcher ownership change
- no selector gate
- no broker submit/cancel gate
- no order/position/proof/queue mutation authority

Telemetry/snapshot failure is fail-soft relative to trading and must be visible diagnostically.

## Required identity
Preserve exact client, execution mode, signal/canonical signal, local order, lifecycle/materialization generation, trigger timestamp, and snapshot parent identity.

Late results may enrich telemetry only for the same proven generation and may never retroactively cancel or authorize a broker decision.

## Required tests
- cache/frozen evidence available -> #615 enrichment persisted;
- cache miss/unavailable -> existing selector path proceeds with UNKNOWN/MISSING;
- provider/network functions are asserted not called synchronously by the bridge;
- snapshot write failure -> trade path unchanged;
- stale generation -> zero enrichment mutation and zero money-path change;
- accepted handoff -> #615/#625/#621 failure releases the retry reservation,
  while durable success/duplicate retains it;
- restart/uninterrupted identity parity;
- normalized structure-view parity across uninterrupted, restart, and
  materialized/deferred inputs;
- duplicate identity includes the complete #625 semantic tuple, including
  profile/model/phase, while generation fencing remains isolated;
- the real deferred `_on_entry_trigger()` calls the existing selector exactly
  once for ACCEPTED, bridge-exception, SATURATED/REJECTED, and
  missing/invalid-intelligence bridge outcomes;
- LIVE/PAPER isolation;
- zero duplicate submit/cancel behavior.

## Dependency
Depends on the current-main #615 freezer, #625 assembly owner, #621 durable
adapter, and #327 job/snapshot substrate. The shared P0 inventory includes the
focused #622 runtime-bridge tests in both exact-head and merge-ref runs.
