# P1 Canonical BREACH Setup Intelligence

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is PR 3 in the profitability-intelligence repair stack.

It rebuilds the useful concept from reverted PR #330 on **current main**, but does not reuse or cherry-pick the stale branch. The new implementation must start from the current watcher/execution lifecycle and must remain **observe-only** in its first version.

## Objective

Evaluate whether a scanner-approved setup is still worth entering **at the moment the trigger actually breaches**.

Current architecture proves a setup was previously eligible, watches for trigger breach, and then proceeds toward contract selection. What is missing is a canonical setup-quality decision point between:

`trigger breach confirmed`

and

`contract selection / entry materialization`.

A trigger crossing is necessary. It is not sufficient proof that the trade is still attractive.

The BREACH intelligence profile must answer:

- Did the setup breach cleanly or barely touch?
- Is the move already extended?
- Is meaningful reward still left before the target or structural obstacle?
- Is the higher-timeframe structure aligned?
- Is the lower-timeframe move actually pushing in the intended direction?
- Is price entering an opposing FVG / imbalance / structural wall?
- Is volume confirming the move or fading?
- Is VWAP context supportive or hostile?
- Is the market/sector context helpful, neutral, or adverse?
- Is this a first breach, re-breach, or direction-flip situation?
- Is the setup still valid after overnight/preopen movement?

## Why BREACH is the correct seam

PRETRIGGER intelligence can describe the planned setup.

PREOPEN intelligence can update context before the session.

Neither knows the actual quality of the eventual intraday trigger breach.

The existing stale/reverted PR #330 recognized this and assembled a BREACH profile after `_on_entry_trigger()`. That implementation was later explicitly reverted by #350. Therefore current main must be traced fresh.

The new implementation must attach to the current real breach seam without bypassing:

- watcher ownership
- canonical signal identity
- lifecycle generation fencing
- direction reversal semantics
- selector retry ownership
- broker submit final authority

## Phase contract

The intelligence lifecycle should conceptually become:

`PRETRIGGER -> PREOPEN -> BREACH -> later CONTRACT_SELECTED -> OUTCOME`

This PR implements only `BREACH` and consumes any existing PRETRIGGER/PREOPEN snapshots when available.

It does not implement selected-contract intelligence and does not become authoritative for LIVE admission yet.

## Required frozen BREACH input

At the moment a breach is confirmed, create one immutable input envelope containing only information available at or before that instant.

Minimum identity:

- `client_id`
- `execution_mode`
- `signal_id`
- `canonical_signal_id`
- `local_order_id`
- ticker
- side/direction
- pattern
- timeframe
- watcher/lifecycle generation if current main exposes it
- trigger source / trigger type

Minimum setup geometry:

- trigger price
- stop underlying
- target underlying / primary target
- current underlying at breach
- first breach bid/ask/last or canonical breach price and provenance
- breach timestamp
- trigger-crossed timestamp/provenance
- whether this is initial breach vs re-breach
- direction-reversal/rearm lineage where available

Minimum inherited context pointers:

- PRETRIGGER snapshot id
- PREOPEN snapshot id
- profile version
- config hash
- git commit

Never query outcome information or future bars during BREACH scoring.

## Required intelligence components

### 1. Trigger / breach geometry

Compute:

- breach distance in dollars and percent
- whether price only touched or decisively cleared the trigger
- whether entry is now meaningfully beyond trigger
- distance from current price to stop
- distance from current price to target
- remaining reward/risk (`remaining_R`)
- whether target is already reached/completed
- whether stop geometry is already invalid
- whether setup is too extended relative to the trigger and remaining target

Direction-specific math must be explicit for CALL vs PUT.

Hard safety observations such as `target_already_reached`, impossible geometry, identity conflict, or invalid direction should be clearly separated from profitability advisories.

### 2. Higher-timeframe structure

Use completed candles only.

Required context:

- monthly
- weekly
- daily
- 4h
- 1h

The component should describe alignment, not blindly veto disagreement.

The Strat / higher-timeframe structure should identify:

- directional agreement
- nearby opposing structure
- broad consolidation/chop
- continuation vs reversal context

Do not allow a generic multi-session market trend to become the same class of hard veto that #382 removed.

### 3. Fair value gaps / structural imbalance

Use the repository's existing FVG machinery where valid, but verify current semantics before reuse.

Minimum outputs:

- active aligned 4h FVG support/resistance
- active opposing 4h FVG in the path to target
- active aligned/opposing 1h FVG
- whether current price is inside an opposing gap
- whether target runs directly into an opposing gap
- gap status (`unfilled`, partial, midpoint touched, filled, broken/reclaimed)
- confirmation requirement if the move must reclaim/break an opposing wall

Do not treat midpoint touch as automatic invalidation if the canonical FVG lifecycle defines otherwise.

### 4. 15m directional strength

This is required. Current durable context primarily uses 15m source data but does not turn it into the entry-efficiency confirmation we need.

At BREACH, evaluate recent completed 15m bars for:

- directional closes
- body strength
- follow-through
- rejection wicks
- higher-high/higher-low vs lower-high/lower-low behavior
- range expansion/contraction
- push through trigger vs immediate rejection
- volume accompanying the push

This component must be deterministic and point-in-time.

### 5. 5m confirmation

Add a canonical 5m source specifically for breach quality.

Required examples of positive CALL evidence:

- completed 5m bar closes above trigger
- strong body rather than wick-only breach
- follow-through or successful retest/reclaim
- directional volume confirmation

PUT is symmetric.

Do **not** require a completed 5m candle when the policy intends immediate entry for exceptionally strong breaches unless that behavior is separately encoded. This PR is observe-only, so record both immediate-breach and completed-candle evidence first.

### 6. Volume confirmation / imbalance

The score-profile documentation references volume imbalance concepts, but current production intelligence does not provide a canonical runtime component for this entry decision.

Implement a deterministic evidence component using only point-in-time market data.

At minimum distinguish:

- relative volume
- volume expansion on directional bars
- buy/sell pressure proxy if the data source genuinely supports it
- inability to calculate true bid/ask volume imbalance if Tradier data does not provide the required fields

Do not label an OHLCV approximation as true order-flow imbalance. Preserve provenance honestly.

### 7. VWAP context

Compute:

- current underlying vs VWAP
- direction relative to VWAP
- distance from VWAP
- recent reclaim/rejection if deterministically inferable
- whether VWAP is aligned, neutral, or opposing

VWAP is setup evidence, not automatic authority unless later profitability proof justifies it.

### 8. Market + sector context

Preserve SPY/market and sector ETF context as advisory evidence.

Use current point-in-time change/trend information where available.

Do not resurrect broad regime mismatch as a hard veto.

### 9. Remaining opportunity / extension

This is critical for preventing late entries.

Compute a deterministic `remaining_opportunity` object containing:

- trigger-to-target total distance
- current-to-target remaining distance
- percent of planned move already consumed
- current-to-stop risk
- remaining R
- distance to nearest opposing structural wall
- whether price has already traversed a configurable fraction of the move before contract selection

Do not invent a hard threshold in this PR. Capture the feature and later measure its relationship to outcomes.

## Canonical score

The first implementation may calculate a setup-quality score for ordering/research, but it must obey:

- score is **not a win probability**;
- score is observe-only;
- fixed component maxima/denominator;
- missing data cannot improve the score by shrinking the denominator;
- stale/error/missing components are separately visible;
- no outcome-derived feature enters the score;
- score version is frozen and identified.

Suggested component families:

- trigger geometry
- higher-timeframe alignment
- FVG/path quality
- 15m strength
- 5m confirmation
- volume confirmation
- VWAP context
- remaining opportunity
- market/sector context

Do not include fundamentals/sentiment simply because the old pipeline did. Include them only if a separate point-in-time profitability analysis later proves value.

## Data freshness

Every market component must carry:

- source
- observed timestamp
- age
- status (`AVAILABLE`, `STALE`, `MISSING`, `ERROR`)

A component cannot be marked available merely because a stale cached number exists.

Current breach execution must never block waiting several seconds for a slow DB/network intelligence fetch.

## Latency contract

BREACH capture must not jeopardize execution timing.

Preferred pattern:

1. freeze the breach input synchronously from already available watcher/runtime truth;
2. enqueue observe-only enrichment asynchronously;
3. zero blocking on DB/network history calls in the money path;
4. bounded handoff capacity;
5. handoff saturation/error affects telemetry only, never execution in this PR.

No `Future.result()`, sleeps, thread joins, or synchronous yfinance/LLM/news/fundamentals calls on the breach-to-selector path.

## Durable persistence

Persist a BREACH snapshot with exact identity and immutable feature payload.

Required:

- append/idempotent behavior by phase/revision/input hash
- exact client/mode/canonical-signal identity
- no overwrite of PRETRIGGER/PREOPEN
- parent snapshot pointers
- data-as-of timestamp
- compute timestamp
- git/config lineage
- observe-only=true
- affected_eligibility=false

If persistence fails, execution continues and health telemetry records the loss.

## Required tests

Minimum matrix:

1. CALL clean breach with strong 5m/15m continuation -> high positive evidence.
2. PUT symmetric case.
3. wick-only breach then close back inside -> weak/rejection evidence.
4. target already reached before/at breach -> hard safety observation recorded.
5. remaining R <= 0 -> invalid opportunity observation.
6. current price far beyond trigger with most target consumed -> extension penalty feature.
7. opposing 4h FVG directly in path -> obstacle recorded.
8. aligned 4h FVG supporting entry -> positive structural evidence.
9. opposing gap already reclaimed/broken -> not treated as active obstacle.
10. missing 4h/1h data -> fixed denominator; score not artificially boosted.
11. stale market data -> component stale.
12. 5m unavailable -> missing, not fabricated from 15m.
13. 15m unavailable -> missing.
14. volume data lacks true bid/ask imbalance -> do not claim true imbalance.
15. VWAP unavailable -> missing, not zero/neutral by default.
16. regime disagreement -> advisory only.
17. client identity mismatch on parent snapshot -> parent rejected, diagnostic retained.
18. execution mode mismatch -> parent rejected.
19. canonical signal mismatch -> parent rejected.
20. duplicate asynchronous dispatch -> one idempotent snapshot revision.
21. handoff capacity exhausted -> execution seam returns immediately; zero selector/broker behavior change.
22. enrichment exception -> zero watcher/order/position mutation.
23. future candle accidentally present -> excluded by completed-candle filter.
24. exact breach timestamp proves no future bar enters feature computation.
25. replay same frozen input twice -> deterministic score/hash.

## Production-path proof

The implementation PR must trace and document the exact current-main call path:

`entry watcher -> confirmed breach -> _on_entry_trigger/current equivalent -> frozen BREACH input -> async intelligence handoff -> existing selector/materialization path`

Do not rely on stale #330 line numbers or method assumptions.

## Scope budget

Expected production files:

1. breach intelligence builder/module
2. `ap/intelligence_context_handoff.py` or current equivalent
3. current breach callback seam in `ap_execution_core.py` / active execution module
4. market-data helper only for 5m/15m point-in-time evidence
5. snapshot store only if required for BREACH phase support
6. focused tests + P0 workflow

No more than five production files without explanation.

## Explicit non-goals

This PR must not:

- block LIVE entries from the new score
- change scanner thresholds
- change watcher breach rules
- change selector rules
- change contract selection
- change DTE policy
- change sizing
- call broker submit/cancel
- mutate position/proof state
- change retry limits
- change exits
- use outcome data at scoring time

## Jason safety

Because this version is observe-only:

- Can it newly make Jason enter a trade? **NO**.
- Can it newly block Jason from a trade? **NO**.
- Can it submit/cancel at broker? **NO**.
- Can it change orders/positions/proof/queue lifecycle? **NO** except append-only intelligence snapshot/pointer telemetry explicitly proven non-authoritative.

## Definition of done

For every confirmed trigger breach, we can reconstruct what the market/setup looked like **at that exact moment** and later compare that frozen profile to the exact outcome from PR 1.

Only after that dataset exists should we decide which breach-quality features deserve live authority.

## Release verdict

Current state: **HARD HOLD — docs only.**

Implementation remains observe-only until holdout evidence demonstrates ranking lift / profitability separation.