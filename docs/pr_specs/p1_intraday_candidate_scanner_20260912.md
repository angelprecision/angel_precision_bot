# P1 Intraday Candidate Scanner

## Status
SPEC ONLY / DRAFT / HARD HOLD. No implementation, merge, or deployment authority.

Base: `main@846525a2ee798071bb0079078f0aec871c415677`.

## Purpose
Continuously evaluate a deliberately small, highly liquid ticker universe during RTH and emit deduplicated intraday setup candidates when pure classifiers report a valid developing/completed setup.

Initial universe examples: SPY, QQQ, IWM, AAPL, GOOGL, NVDA, MSFT. Universe membership must be explicit/configured; this PR does not create a broad market-wide scanner.

## Ownership boundary
This PR owns orchestration/deduplication only:

`forming bar state -> pure setup classifier -> canonical candidate`

It does not own bar construction, setup math, FVG intelligence, watcher behavior, selector behavior, or broker execution.

## Required behavior
- Evaluate configured symbols continuously during RTH from existing forming/completed bar state.
- Support 5m/15m/30m/60m setup scans without historical refetch per setup evaluation.
- A developing setup can emit before timeframe close when the classifier says it is structurally valid.
- Re-evaluate when forming-bar state changes.
- If a formerly valid developing setup becomes invalid (for example a developing `2` becomes `3`), emit/update deterministic invalidation state; never continue treating it as actionable.
- Candidate identity must be deterministic across process restart. Minimum identity: symbol + timeframe + setup_type + session date + source bar IDs + direction.
- Same candidate is emitted at most once per state/generation. Repeated polls must not create duplicate opportunities.
- Different symbols, timeframes, directions, or source-bar generations remain independent.
- Scanner restart reconstructs candidate state from canonical bar/classifier facts rather than inventing a new economic opportunity.
- Missing/stale bar state yields no new candidate and cannot terminalize unrelated opportunities.

## Candidate output
Emit a side-effect-limited canonical candidate record/event containing:
- candidate_id
- ticker
- timeframe
- setup_type
- setup_state
- direction
- source bar IDs/timestamps
- trigger/invalidation geometry supplied by classifier
- discovered_at
- source_state_version/generation
- `observe_only=true` initially
- `affected_eligibility=false` initially

Initial implementation must remain observe-only. Promotion into the existing signal/watcher pipeline requires a separate PR after production replay.

## Explicit non-scope
No market-data transport redesign, no bar aggregation logic, no setup classification math, no FVG/VI policy, no Master Control eligibility changes, no watcher installation, no selector calls, no broker submit/cancel/replace, no order/position/proof mutation, no risk/sizing changes, and no broad ticker expansion.

## Mandatory tests
1. One developing 60m 2-3-2 candidate emitted when the third bar becomes `2U`/`2D`.
2. Repeated unchanged evaluations emit no duplicate.
3. Third bar changes to `3` -> candidate invalidated and cannot remain valid.
4. Later new bar generation can create a new distinct candidate.
5. Same ticker 15m and 60m candidates stay independent.
6. Same timeframe opposite direction stays independent when source bars differ.
7. Multi-ticker isolation.
8. Restart reproduces same candidate ID and no duplicate emission.
9. Missing/stale source state -> no new candidate.
10. Configured-universe fence rejects unapproved symbols.
11. No history-fetch fanout per classifier/setup.
12. Zero watcher/selector/broker/order/position/proof calls.

## Performance target
For the initial liquid universe, scanner orchestration/classification should be cheap enough to reevaluate on each accepted state update or a bounded minute cadence without rebuilding full histories. Expensive structural intelligence remains cached/precomputed elsewhere.

## Merge gate
Implementation remains observe-only and separate from execution. Exact-head and merge-ref P0/cohesion must pass on one unchanged SHA. Any watcher/broker authority entering this PR is a HARD HOLD.
