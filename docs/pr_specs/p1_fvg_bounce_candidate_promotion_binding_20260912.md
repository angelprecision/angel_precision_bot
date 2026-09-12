# P1 FVG Bounce Candidate Promotion Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC FIRST. DO NOT MERGE OR DEPLOY.**

Base at creation:
`main@71f25dabf739bce3360593ac44a7c7bf71ef9513` (#625 merged).

## Stack position

`#614 -> #615 -> #621 -> #622 -> FVG hold/bounce classifier -> THIS PR -> existing Angel Precision watcher/selector/broker pipeline`

This PR owns only the narrow promotion seam from a proven FVG hold/bounce opportunity into the existing candidate/signal lifecycle.

## Purpose

Take a deterministic, authoritative FVG hold/bounce classification that has already been proven by the dedicated classifier and translate it into the existing Angel Precision opportunity format without creating a second execution system.

The promotion layer must preserve existing watcher, selector, risk, sizing, broker, order-state, position, and proof ownership.

## Preconditions for promotion

Promotion is permitted only when all of the following are true:
- classifier result is an explicitly actionable `CANDIDATE` / approved equivalent;
- classifier result is not `UNKNOWN`, `NO_SETUP`, or `INVALIDATED`;
- exact ticker and direction are present;
- exact source FVG identity/timeframe/geometry are present;
- exact decision/evidence as-of timestamp is present;
- lower-timeframe authority required by classifier contract is proven;
- no authoritative strong break has invalidated the FVG support/resistance thesis;
- deterministic candidate identity can be formed without joining by ticker/time proximity.

Unknown/missing intelligence must produce **no new FVG-generated opportunity**, but must never block or mutate unrelated existing opportunities.

## Canonical candidate identity

Bind enough immutable structure to represent one economic FVG opportunity generation, including where available:
- ticker;
- direction / side mapping;
- source FVG timeframe (4H or 1H initially);
- canonical FVG identity;
- FVG boundaries/geometry hash or equivalent immutable identity;
- session/trading date;
- classifier version;
- exact decision/evidence as-of;
- source structure generation/version;
- promotion version.

Do not deduplicate by ticker alone.

The same ticker may legitimately have:
- a CALL candidate from bullish FVG support;
- a PUT candidate from bearish FVG resistance;
- distinct candidates from different canonical FVGs/timeframes.

Those must remain independent unless they are provably the same economic opportunity.

## Existing pipeline reuse

Promotion must feed the existing Angel Precision opportunity/signal/watcher lifecycle.

Do not create:
- a second watcher;
- a second selector;
- direct broker submission;
- a bespoke FVG order path;
- an FVG-specific position manager;
- an alternate proof ledger.

After promotion, all normal downstream authority remains where it already lives:

`promoted FVG candidate -> existing watcher/materialization -> existing selector/final gates -> existing broker/order state machine -> existing exit/proof/reconciliation`

## Tradeflow contract

This PR is additive opportunity generation.

It must not:
- globally gate existing overnight or intraday candidates;
- change eligibility of unrelated setups;
- terminalize existing watchers;
- consume or replace another scanner's candidate simply because the ticker matches;
- require FVG intelligence for non-FVG setup families;
- turn missing classifier output into a global trade veto.

A failure in this promotion seam means only that the new FVG-generated opportunity was not promoted. Existing tradeflow continues unchanged.

## Direction mapping

Initial mapping:
- bullish FVG support hold/bounce -> CALL-direction opportunity;
- bearish FVG resistance hold/rejection -> PUT-direction opportunity.

No automatic reversal logic is authorized beyond the classifier's proven direction.

## Lifecycle / reevaluation

Promotion must preserve the source classifier generation and avoid duplicate opportunities.

Required behavior:
- identical classifier candidate repeated -> same canonical promoted candidate, no duplicate watcher/install;
- restart with identical durable facts -> same candidate identity;
- classifier invalidation before downstream trigger -> candidate is withdrawn/invalidated through the existing lifecycle seam, not by deleting rows ad hoc;
- newer genuine source generation may create a new opportunity;
- later current price alone cannot silently rewrite the original candidate identity.

Do not impose a generic second-touch requirement.

## Interaction with defensive FVG WAIT policy

This promotion PR creates opportunities from aligned FVG support/resistance behavior.

The separate defensive FVG policy may still evaluate any promoted candidate using its own exact rules.

Do not merge the two concepts:
- **aligned FVG hold** can create a candidate;
- **opposing FVG without acceptance** can delay a candidate.

A promoted FVG bounce setup must not receive magical bypass authority over downstream safety/selector gates.

## Explicit non-scope

No:
- FVG detection math;
- hold/bounce classification math;
- 5m/15m strong-break calculation;
- market-data fetching;
- new snapshot persistence;
- selector rewrite;
- spread/liquidity gate changes;
- sizing/risk changes;
- broker changes;
- exit changes;
- global Master Control gating;
- overnight-scanner redesign;
- 2-3-2 / 3-2-2 classifier changes;
- broad #436 policy import.

## Required proof

At minimum:
1. bullish 4H FVG candidate promotes once into CALL-direction existing pipeline input;
2. bearish 4H FVG candidate promotes once into PUT-direction existing pipeline input;
3. 1H equivalents remain identity-distinct from 4H where canonical FVG differs;
4. repeated identical classification does not duplicate candidate/watcher;
5. restart reproduces same promoted candidate identity;
6. same ticker CALL and PUT opportunities stay independent;
7. same ticker different FVG identities stay independent;
8. UNKNOWN/NO_SETUP/INVALIDATED never promotes;
9. malformed/missing identity fails only this new opportunity and does not block unrelated tradeflow;
10. classifier invalidation before execution converges through existing lifecycle semantics;
11. no direct selector/broker/order/position mutation before existing pipeline owns it;
12. no regression to overnight/intraday candidate throughput;
13. LIVE/PAPER/client isolation remains exact when the existing downstream pipeline materializes the candidate;
14. fail-soft behavior under promotion persistence/dispatch error;
15. exact-head and restart/idempotency proof on production-shaped fixtures.

## Dependencies / merge gate

Do not implement or merge production authority until:
1. #621 is merged;
2. #622 is merged;
3. the FVG hold/bounce classifier is independently implemented, replayed, and cleared;
4. the existing candidate/watcher promotion seam is traced on then-current main.

Before production edits, post the exact trace:

`FVG classifier result -> canonical promotion identity -> existing candidate/signal representation -> watcher/materialization owner -> selector boundary`

If implementation requires a new execution subsystem or broad changes to master control/watcher/selector, STOP and keep HARD HOLD.

Final clearance requires focused tests, existing watcher/materialization regression suites, exact LIVE/PAPER isolation proof, exact-head P0/cohesion, genuine merge-ref parity, `git diff --check`, and whole-PR money-path audit on one unchanged SHA.
