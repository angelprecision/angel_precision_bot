# P1 FVG Entry Admission Policy

## Status
SPEC ONLY / DRAFT / HARD HOLD. No implementation, merge, or deployment authority.

Base: `main@846525a2ee798071bb0079078f0aec871c415677`.

## Purpose
Consume the observe-only BREACH market-structure evidence produced by #615 and apply one narrow entry-admission decision before selector/broker-ready handoff.

This PR owns policy only. It does not own market-data acquisition, FVG detection, setup discovery, bar construction, contract selection, broker submission, or exit behavior.

## Required policy
For an otherwise valid entry candidate:
- If no relevant opposing FVG is proven, preserve the existing path.
- If a relevant opposing FVG is proven and no trustworthy strong completed 5m/15m break is proven, return `WAIT_FVG_BREAK` / retain watcher ownership; do not terminalize the opportunity and do not submit.
- A strong break requires the exact #615 evidence contract: completed 5m or 15m close beyond the relevant far boundary, directional real body, >=50% of real body beyond boundary, and body >=50% of full candle range.
- Wick-only penetration is not acceptance.
- Pullback/reclaim/re-breach may keep/rearm the same valid opportunity only under existing identity/generation ownership; never fabricate a new economic opportunity from stale evidence.
- `MISSING`, `UNKNOWN`, timeout, stale cache, provider failure, or late intelligence is not positive adverse evidence and must not by itself terminalize/cancel/exhaust an otherwise valid watcher.
- Under bounded intelligence latency, fall back according to the existing trade path contract rather than blocking indefinitely. Late results cannot retroactively cancel a broker decision already submitted.

## Hot-path performance
Do not refetch/rebuild full 4H/1H/15m/5m history synchronously at breach. Consume precomputed/cached point-in-time structure and only the small latest completed-bar evidence required for the decision.

Target: approximately <=250ms p95 Angel-Precision-owned compute between confirmed breach and selector invocation, excluding external network latency.

## Identity / ownership invariants
Preserve exact client_id, execution_mode, canonical signal identity, local_order_id, watcher generation/ownership, and trigger authority. A stale generation or ownership loss yields zero policy mutation and zero selector/broker call.

## Explicit non-scope
No scanner logic, no intraday setup classifier, no ticker-universe expansion, no contract quality gate changes, no spread/OI/volume/delta/DTE/premium changes, no sizing/risk changes, no broker adapter redesign, no exit changes, no proof/position/queue mutation.

## Mandatory tests
1. NOW-shaped PUT at bullish FVG: wick through then close back inside -> `WAIT_FVG_BREAK`, watcher retained, selector/broker calls = 0.
2. Strong completed 5m body acceptance -> continue existing path exactly once.
3. Strong completed 15m body acceptance -> continue existing path exactly once.
4. Missing/unknown intelligence -> does not terminalize watcher solely for missing intelligence.
5. Provider timeout/cache miss -> bounded fallback; no indefinite hold.
6. Late intelligence after submit -> no retroactive cancel/mutation.
7. Pullback/reclaim/re-breach retains exact opportunity identity.
8. Restart/process death reaches same decision from durable evidence.
9. Stale generation/ownership loss -> zero mutation and zero broker/selector work.
10. LIVE/PAPER intelligence taxonomy identical; execution identity preserved.
11. Same ticker concurrent clients do not leak evidence/identity.
12. No position/proof before exact fill.
13. Existing duplicate-submit fence remains intact.

## Merge gate
This is the first money-path PR in this stack and requires production-shaped replay, unchanged-head exact P0 + merge-ref P0, ownership/restart tests, latency evidence, and a final full diff audit. Any market-data or setup-scanner redesign entering this PR is a HARD HOLD.
