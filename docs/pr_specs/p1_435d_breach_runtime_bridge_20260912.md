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

## Critical latency invariant
The bridge may not synchronously fetch market history or wait on Tradier, option chain, external APIs, futures, sleeps, joins, or background results before selector execution.

If the exact #614 evidence is not already attached to the watcher/plan, record
an explicit `UNKNOWN/MISSING` diagnostic artifact and continue the existing
trade path unchanged. #615 runs only in the bounded background handoff.

If the current positive materialization generation cannot be proven, keep the
bridge diagnostic-only and do not hand off an identity-less sideband artifact.

A cache miss must never become a selector delay.

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
- restart/uninterrupted identity parity;
- LIVE/PAPER isolation;
- zero duplicate submit/cancel behavior.

## Dependency
Depends on the current-main #615 freezer, #625 assembly owner, #621 durable
adapter, and #327 job/snapshot substrate. The shared P0 inventory includes the
focused #622 runtime-bridge tests in both exact-head and merge-ref runs.
