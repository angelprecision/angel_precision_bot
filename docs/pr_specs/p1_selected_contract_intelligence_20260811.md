# P1 Real Selected-Contract Intelligence

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is PR 5 in the profitability-intelligence repair stack.

It replaces the current false pre-selector idea of “contract intelligence” with evaluation of the **actual durable contract selected by the real selector**.

Do not reuse stale PR #331 directly. #331 is open/draft, was based on the reverted #330 intelligence branch, and must be rebuilt on current main after the new BREACH profile exists.

## Objective

After the setup is `READY_NOW` and the selector chooses a real OCC contract, intelligence should answer:

> Is this exact contract/executable price a good vehicle for this otherwise valid setup right now?

This is a separate question from whether the underlying setup is good.

A good underlying setup can still produce a bad options entry because:

- spread is too wide;
- executable ask moved materially above the quote used during selection;
- delta is outside the intended participation range;
- liquidity is thin;
- DTE is wrong for the policy;
- selected strike is too far from the trigger/target geometry;
- premium has already expanded after the breach;
- expected move / IV context makes the planned target unrealistic or poor value;
- quote is stale;
- contract identity or mode metadata is inconsistent;
- affordability/sizing changed while the setup waited.

The selector remains the canonical contract-selection engine. This PR does not create a second selector. It evaluates the selector's chosen durable truth and can initially remain observe-only.

## Required placement

Trace current main first.

Preferred seam:

`READY_NOW -> selector -> durable broker-ready order row / exact selector handoff proof -> CONTRACT_SELECTED intelligence snapshot -> existing final submit gates -> broker POST`

The intelligence evaluator must read the exact durable selected-contract evidence that the money path is about to use, not an in-memory object that can diverge from persisted order identity.

Do not evaluate a placeholder such as `DEFERRED:*` as a real contract.

## Identity invariant

A successful `CONTRACT_SELECTED` intelligence observation must bind exactly to:

- client id
- execution mode
- canonical signal id
- signal id
- local order id
- selected OCC contract
- ticker
- CALL/PUT direction
- quantity
- selected execution/limit price
- materialization/broker-ready generation if current main uses one
- parent BREACH snapshot id

Any identity conflict -> no successful intelligence snapshot and zero new authority.

Never repair identity by guessing from ticker, client environment, or one matching contract.

## Real contract evidence required

Consume the actual selected values where present:

- OCC symbol
- underlying ticker
- call/put
- expiration
- DTE
- strike
- bid
- ask
- midpoint for analytics only
- actual intended BUY execution price
- quote timestamp
- quote source/domain
- quote age
- spread dollars
- spread percent
- delta
- gamma
- theta
- implied volatility
- open interest
- option volume
- underlying price at quote
- ATM IV when available
- quantity
- premium per contract
- estimated total debit
- reserved cost
- selector candidate diagnostics
- expiration-policy match
- strike-policy match
- distance from selected strike to the canonical strike/trigger policy

Unknown fields must be `MISSING`, not invented from defaults.

## Execution-price authority

Angel Precision buys options.

For entry quality, the relevant executable side is ASK or the exact current BUY limit policy, not BID.

Midpoint may remain an analytical fair-value reference but cannot prove affordability or executable entry quality in LIVE.

Required diagnostics:

- bid
- ask
- mid
- intended limit
- slippage from mid to intended limit
- spread dollars/percent
- change from selector-observed ask to final-submit revalidation ask

Do not use exit-side BID authority for BUY entry decisions.

## DTE semantics

Preserve explicit `0` as 0.

Do not use truthy fallback that converts 0DTE to 1DTE.

Contract evidence must prove expiration/DTE from actual selected contract or selector metadata.

If DTE cannot be proven, intelligence cannot call the contract exact/complete.

## Contract-quality components

### 1. Liquidity

Evaluate actual:

- spread percent
- OI
- option volume
- quote freshness
- quote completeness

Selector's hard liquidity rules remain authoritative unless separately changed by reviewed selector PR.

This intelligence layer may score *quality within the allowed range* but must not silently loosen selector hard gates.

### 2. Delta / strike participation

Capture:

- selected delta
- intended delta policy range
- strike distance from underlying
- strike relation to trigger/target
- ITM/ATM/OTM classification

Do not change strike selection in this PR.

Measure whether contracts near one part of the allowed delta band outperform others before adding new hard thresholds.

### 3. IV / expected move

Use real selected IV only when available with source/timestamp.

Compute expected move using a documented formula and units.

Record:

- one-sigma underlying move for DTE
- planned remaining underlying target distance
- ratio of remaining target distance to expected move
- IV level relative to available context if point-in-time history exists

Do not treat missing IV as zero.

Do not invent IV from option price.

### 4. Premium expansion / chase risk

Compare, when exact point-in-time evidence exists:

- option quote near initial breach/selection
- current executable ask immediately before submit
- percent/dollar expansion
- underlying move since breach
- remaining opportunity after the option repriced

This is where entry intelligence can catch “good setup, terrible premium after the move.”

Any future authoritative chase rule must preserve the existing final submit/retry contracts and cannot conflict with post-cancel #430 without explicit integration tests.

### 5. Theta / DTE efficiency

Capture theta and DTE for outcome analysis.

Do not assume lower theta is always better; 0DTE/1DTE/index behavior differs.

The score can record a normalized cost-of-time feature only after units and data availability are verified.

### 6. Affordability / position economics

Use exact account authority from Master Control / existing sizing path.

Intelligence may record:

- selected quantity
- debit
- percent of account/equity if canonical value is available
- remaining buying power after reservation

It must not re-size independently or use generic `$25k` environment equity.

### 7. Selector ranking audit

Preserve enough selector diagnostics to later answer:

- how many candidates were considered
- why alternatives were rejected
- where selected contract ranked
- whether the selector stopped due to quote/API budget
- whether a better-ranked candidate was unavailable vs quality-rejected

Do not duplicate huge unbounded chain payloads in order metadata. Store compact diagnostics/pointers.

## Canonical output

Persist a `CONTRACT_SELECTED` snapshot linked to the parent BREACH snapshot.

Suggested outputs:

- `setup_score` from BREACH profile, immutable pointer/reference
- `contract_quality_score` / `execution_score`
- `combined_research_score` only if components and denominator are fixed/versioned
- `profile_status`
- `component_statuses`
- `hard_safety_blocks`
- `strategy_advisories`
- `data_quality_warnings`
- exact contract evidence
- exact quote provenance
- identity hashes/pointers
- observe_only
- affected_eligibility

Do not call any score a probability of winning.

## Authority rollout

Phase 1: `observe_only`.

Phase 2: PAPER authoritative for narrowly proven contract/execution-quality checks.

Phase 3: LIVE authority only after exact outcome-bound evidence proves improvement.

Hard selector gates remain independent. The new score should not duplicate every selector gate and create contradictory authorities.

Potential future authoritative uses after proof:

- reject stale selected quote
- reject extreme premium chase after breach
- reject materially degraded remaining opportunity
- force reselect when contract quote/quality changed beyond reviewed tolerance

Do not enable those in the first implementation merely because they sound sensible.

## Revalidation before broker submit

If any contract-intelligence decision is later made authoritative, it must be based on current quote truth close to submission.

A `CONTRACT_SELECTED` snapshot taken 30+ seconds earlier cannot by itself authorize a LIVE POST after price moved.

The existing final submit guard remains the last money-path authority.

If final revalidation differs materially:

- HOLD / reselect / terminalize according to existing reviewed policy;
- do not blindly submit using stale intelligence;
- do not invent a new broker retry owner.

## Required tests

Minimum:

1. real OCC contract + exact identity -> successful snapshot.
2. `DEFERRED:*` placeholder -> no successful contract snapshot.
3. missing OCC -> error/missing.
4. client mismatch -> reject evidence.
5. execution-mode mismatch -> reject evidence.
6. canonical-signal mismatch -> reject evidence.
7. local-order mismatch -> reject evidence.
8. contract mismatch between plan and durable row -> reject evidence.
9. selected price mismatch beyond exact tolerance -> error, never silently choose one.
10. explicit DTE=0 preserved.
11. stale quote -> component stale.
12. missing ask -> cannot claim executable BUY quality.
13. midpoint present but ask missing -> analytics only, no executable approval.
14. wide spread already rejected by selector -> intelligence does not loosen it.
15. narrow spread + good liquidity -> positive quality evidence.
16. OI missing -> missing, not default 0/pass.
17. volume missing -> missing.
18. IV missing -> expected-move unavailable.
19. real IV + DTE + underlying -> expected move deterministic.
20. premium expansion after breach -> chase feature accurately computed.
21. parent BREACH snapshot identity mismatch -> no lineage claim.
22. duplicate dispatch same durable order revision -> idempotent snapshot.
23. selector retry creates a new materialization generation/contract -> old snapshot cannot authorize new contract.
24. reselect changes OCC -> new snapshot required.
25. final quote moves after snapshot -> later authoritative mode must revalidate, stale snapshot not enough.
26. PAPER contract can never produce LIVE snapshot.
27. missing canonical account equity -> no fake percent-of-equity calculation.
28. order quantity/reserved-cost inconsistency -> diagnostic/hold according to current canonical path, never intelligence-side repair.
29. broker submit/cancel call count from observe-only evaluator -> zero.
30. no position/proof/queue mutations.

## Outcome analysis required before promotion

Using PR 1 exact binding, report performance by:

- spread bucket
- delta bucket
- DTE
- IV bucket
- premium size
- entry chase/expansion bucket
- remaining R at contract selection
- contract execution score decile
- setup score x contract score matrix

Required metrics:

- sample count
- win rate
- average return
- average win
- average loss
- expectancy
- fill rate
- MFE/MAE where canonical

This is how we determine which contract features actually separate winners from losers instead of tuning to vibes.

## Scope budget

Expected production files:

1. selected-contract intelligence builder/module
2. current selector/broker-ready handoff seam in `ap_execution_core.py` or equivalent
3. intelligence snapshot store only if new phase support needed
4. compact selector diagnostic extraction helper only if necessary
5. tests + P0 workflow

Do not modify selector ranking/gates in this PR unless implementation proves a separate selector defect, in which case stop and open a separate PR.

## Explicit non-goals

No changes to:

- scanner
- trigger/breach rules
- entry-efficiency policy
- selector quality thresholds
- selector budgets
- DTE ladder
- position sizing formula
- broker adapter
- broker POST/cancel API shape
- post-cancel retry #430
- exits
- proof taxonomy
- queue fanout

## Money-path safety

Initial observe-only implementation:

- LIVE behavior changed: **NO**
- broker submit/cancel added: **NO**
- orders mutated: **NO**, except optional append-only intelligence pointer after durable row proof if strictly identity-fenced
- positions mutated: **NO**
- `proof_trades` mutated: **NO**
- queue mutated: **NO**
- `client_id` / `execution_mode` preserved: **YES, exact/fail closed**

Future authority must receive a separate explicit promotion review.

## Jason safety question

Could this initial PR make Jason trade junk? **NO**, because observe-only output cannot change admission or broker submit.

Could a later promoted version stop a bad premium/contract from reaching submit? Potentially yes, but only after exact outcome evidence proves the rule and the final-submit/retry integration is tested.

## Definition of done

For every selected contract we can prove exactly what Jason was about to buy, what the executable market looked like, how that contract related to the setup, and later whether those features predicted profitable outcomes.

## Release verdict

Current state: **HARD HOLD — docs only.**

Rebuild on current main after the BREACH profile exists. Observe-only first.