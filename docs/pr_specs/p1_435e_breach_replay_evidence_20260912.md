# P1 435-E — BREACH Replay / Evidence Closure

## Status
DRAFT / HARD HOLD / SPEC ONLY. Do not merge or deploy.

## Purpose
Close #435 with production-shaped evidence before any timing policy receives authority.

This PR is replay/evidence only. It does not add trading authority.

## Mandatory incident replays
Use exact durable identities for:
- NOW
- QQQ
- HOOD
- LULU

Add positive/negative controls from the same stack.

For each opportunity reconstruct only from contemporaneous/frozen evidence:

`#614 PIT candles -> #615 structure -> first breach -> FVG relationship -> 5m/15m wick/body acceptance -> pullback/reclaim/re-breach -> actual subsequent path/outcome`

Never use later market data to rewrite what the decision-time snapshot knew.

## Restart parity
For the same durable opportunity and same evidence, uninterrupted execution and restart/recovery must produce the same:
- snapshot identity
- as-of
- relevant FVG/zone identity
- penetration classification
- pullback/reclaim/re-breach state
- schema/model version
- structure/input hash

## Required negative controls
- wrong client
- wrong execution mode
- wrong canonical signal
- wrong local order
- stale lifecycle/materialization generation
- same ticker nearby time but different opportunity
- missing/stale provider evidence
- later candle that did not exist at decision time

## Counterfactual boundary
No hindsight-selected hypothetical fill. Replay records facts. A delayed-entry counterfactual requires a prospective versioned shadow policy plus contemporaneous executable evidence in later stack PRs.

## Closure criterion
If #614 + #615 + existing #327/#330 snapshot/runtime machinery can reproduce the required incidents with deterministic restart parity and zero money-path change, historical monolith #435 is considered replaced and must never be merged wholesale.

## Safety
Zero broker submit/cancel, selector/risk changes, watcher changes, order/position/proof/queue mutation, or LIVE promotion authority.
