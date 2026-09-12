# P1 435-D — Observe-Only BREACH Runtime Bridge

## Status
DRAFT / HARD HOLD / SPEC ONLY. Do not merge or deploy.

## Purpose
Reuse the BREACH dispatch seam already merged in #330. Do not create a second execution path.

Canonical path:

`existing confirmed BREACH -> consume already-available #614 PIT/cached evidence -> run #615 freeze -> append/enrich existing BREACH snapshot`

## Critical latency invariant
The bridge may not synchronously fetch market history or wait on Tradier, option chain, external APIs, futures, sleeps, joins, or background results before selector execution.

If the exact #614 evidence is not already available/cached/frozen inside the allowed budget, record `UNKNOWN/MISSING` and continue the existing trade path unchanged.

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
Implement after #615 and 435-C are cleared.
