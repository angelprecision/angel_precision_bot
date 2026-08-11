# P1 Entry-Efficiency Recheck Lifecycle

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is PR 4 in the profitability-intelligence repair stack.

This PR is where intelligence may eventually begin to change entry timing, but the first implementation must be guarded and evidence-driven. It must not simply add another retry loop or delay every trade.

## Problem

Angel Precision currently treats a confirmed trigger breach as a handoff toward contract selection/execution.

That is too coarse for profitability.

A setup can be valid and still produce a poor immediate entry:

- first breach is wick-only and fails;
- price crosses trigger after already consuming most of the planned move;
- price needs one 5m confirmation/retest before continuation;
- breach happens into opposing structure or VWAP;
- first breach fails but the setup remains structurally valid and later re-breaches cleanly;
- option selection/submission fails once while the underlying thesis remains valid;
- direction briefly reverses and later re-arms under the current lifecycle.

The system needs a bounded distinction between:

1. **the setup is invalid**, and
2. **the setup is valid but the immediate entry is inefficient**.

Those are not the same outcome.

## Core invariant

> A valid setup should not be forced into an immediate bad entry merely because the trigger touched, and it should not be permanently discarded merely because the first executable moment was poor.

At the same time:

> Entry-efficiency logic must never force a stale setup through after current market truth says the opportunity is gone.

## Lifecycle model

Do not create an independent retry subsystem.

Reuse the current durable watcher / pending-trigger / materialization ownership architecture and represent entry-efficiency state as a bounded continuation of the same economic opportunity.

Conceptually:

`WATCHING`

-> confirmed trigger breach

-> `BREACH_EVALUATING`

then exactly one of:

- `READY_NOW`
- `WAIT_CONFIRMATION`
- `REARM_FOR_REBREACH`
- `TERMINAL_INVALID`

`WAIT_CONFIRMATION` may later become:

- `READY_NOW`
- `REARM_FOR_REBREACH`
- `TERMINAL_INVALID`
- `EXPIRED`

Names may differ if current OSM/watcher lifecycle has canonical states. Do not add public order statuses casually. Prefer durable metadata/substate where existing state machines require stable enums.

## Authority split

This PR must preserve three independent concerns:

### Static setup approval

Facts already proven before breach and unchanged by a short wait do not need to be re-earned from scratch.

Examples:

- originating scanner setup identity
- original pattern/timeframe
- canonical signal
- previously approved static eligibility

### Dynamic market truth

Must be revalidated before any later broker POST:

- direction relative to trigger
- trigger/breach geometry
- target not already completed
- stop/setup not invalid
- fresh underlying quote
- 5m/15m confirmation state
- remaining opportunity
- structural obstacle state
- selected contract validity if a contract already exists
- account risk/capital
- no conflicting active order/position

### Broker-submit final authority

Existing final submit gates remain authoritative immediately before broker submission.

Entry-efficiency logic may decide **when to try**. It may not bypass the money-path guards that decide **whether this exact order may submit**.

## Decision classes

### READY_NOW

Used when the breach profile indicates sufficient immediate continuation quality.

Possible positive evidence, subject to calibration from PR 3 data:

- decisive trigger clearance
- strong completed 5m close through trigger or equivalent immediate-strength evidence
- supportive 15m structure
- strong remaining R
- no near opposing 4h/1h obstacle
- aligned VWAP/volume context
- move not already overextended

Do not hardcode final weights/thresholds before outcome analysis. First implementation may use a conservative reviewed ruleset behind a flag.

### WAIT_CONFIRMATION

Used when the setup is still valid but immediate entry quality is ambiguous.

Examples:

- wick-only breach
- small marginal breach without follow-through
- 5m candle still forming and policy requires confirmation
- price at VWAP/near structural wall but not invalid
- move temporarily pauses after crossing trigger
- volume confirmation weak but not adverse enough to kill setup

This state must retain one durable owner and schedule bounded reevaluation without creating duplicate watcher/order owners.

### REARM_FOR_REBREACH

Used when the initial breach no longer qualifies as an entry, but the underlying setup remains valid if price later crosses the trigger again under the current reversal/rearm contract.

Examples:

- price crosses trigger then falls back to pre-trigger side before entry
- direction-reversal logic says opportunity should return to watcher ownership
- confirmation window expires without invalidating original setup

This must use the current canonical rearm semantics, not invent a parallel watcher registry.

### TERMINAL_INVALID

Examples:

- target already completed
- scanner/setup stop invalid under the current trigger-activated semantics
- impossible/negative remaining reward
- market session/time cutoff exceeded
- identity conflict
- setup explicitly invalidated by current canonical direction-reversal policy
- account risk no longer allows entry

Terminalization must preserve a structured reason and must not silently convert to a retry.

## Bounded timing

No unlimited waiting.

Define one explicit opportunity window from confirmed breach or initial evaluation. The exact value must be evidence-backed and configurable.

The implementation PR must not casually reuse unrelated retry settings such as:

- selector retry max attempts
- broker cancel/retry limits
- DB retry limits
- post-cancel order retry timers

Entry-efficiency reevaluation is a distinct strategy continuation concept.

Suggested first research bounds to test, not blindly deploy:

- 5m confirmation: up to one completed 5m bar after breach
- optional second short reevaluation if re-breach occurs within a bounded opportunity window
- hard session/time cutoff remains authoritative

The final implementation value must be justified by historical replay and current live opportunity cadence.

## Data inputs

Consume the canonical BREACH intelligence profile from PR 3 when available.

At every reevaluation refresh only dynamic evidence:

- underlying quote + age/source
- trigger relation
- current 5m/15m completed bars
- VWAP
- volume confirmation
- current remaining opportunity
- current active FVG/structural obstacle state if the component can update safely

Do not refetch expensive fundamentals/news/sentiment/LLM data.

Do not wait on Supabase to calculate market truth if it can be obtained from the broker/data source and existing in-memory runtime state.

## Supabase latency isolation

Production logs on 2026-08-11 show some order/retry queries taking 10-15 seconds and statement timeouts.

Entry-efficiency must not become dependent on a slow DB round trip for every quote tick.

Durable ownership writes are necessary, but market confirmation should use bounded runtime data and the minimum exact DB/CAS operations needed for ownership.

Implementation must measure and log:

- evaluation compute duration
- DB ownership duration
- market-data duration
- time from breach to `READY_NOW`
- time from breach to terminal/rearm

If the DB cannot durably claim/transition the state within its bounded policy, fail closed on new broker submission. Never fall back to process-local duplicate authority.

## No duplicate ownership

At every point there must be exactly one durable owner of the economic entry opportunity.

Required identity includes current-main canonical fields, at minimum:

- client id
- execution mode
- canonical signal id
- signal id where retained
- local entry order id / pending owner
- breach/lifecycle generation where current main requires it
- retry/continuation generation if introduced

Duplicate worker ticks must not produce two `READY_NOW` promotions or two broker submissions.

## Interaction with post-cancel retry #430

Current main includes #430.

Do not conflate:

- **pre-submit entry-efficiency wait** with
- **post-broker-submit cancel/retry continuation**.

If an order was already accepted by the broker and later canceled unfilled, #430 owns that continuation lifecycle.

This PR owns decision timing **before** a new broker submit or after a canonical rearm that has returned ownership to watcher/pre-submit state.

No second post-cancel retry owner may be introduced.

## Interaction with selector

Preferred flow:

`breach -> entry efficiency decision -> READY_NOW -> selector -> final contract intelligence -> submit`

Do not run the expensive selector repeatedly while merely waiting for a 5m confirmation unless current architecture proves contract preselection is safe and non-authoritative.

If a previously selected contract becomes stale during a wait, it must be revalidated/reselected under selector policy before submission.

## Feature flag / rollout

First active implementation must be behind an explicit rollout control.

Suggested modes:

- `observe_only`: calculate what decision would have occurred, execution unchanged.
- `paper_authoritative`: changes PAPER timing only.
- `live_authoritative`: unavailable until explicit promotion gate is satisfied.

Malformed/unset mode should default to `observe_only` initially.

Do not make LIVE authoritative merely by setting a generic `INTELLIGENCE_ENABLED=1` flag.

## Promotion criteria

LIVE authority requires evidence from exact outcome-bound data.

Minimum required report before promotion:

- number of eligible breaches observed
- number `READY_NOW`, `WAIT`, `REARM`, `TERMINAL`
- realized outcome of actual immediate entries
- counterfactual outcome of delayed/recheck path where reconstructable without leakage
- fill-rate impact
- missed-winner rate introduced by waiting
- avoided-loser rate
- average entry-price improvement/degradation
- win-rate delta
- average win/loss delta
- expectancy delta
- maximum adverse excursion delta
- sample sizes with uncertainty

No promotion from anecdotes such as “three delayed trades looked better.”

## Required production-shaped replays

Minimum:

1. strong immediate CALL breach -> READY_NOW.
2. strong immediate PUT breach -> READY_NOW.
3. wick-only CALL breach -> WAIT; later 5m confirmation -> READY_NOW.
4. wick-only breach -> falls back below trigger -> REARM, zero submit.
5. re-breach cleanly -> one new READY transition, one eventual submit maximum.
6. waits then target completes -> terminal, zero submit.
7. waits then stop/setup invalidates -> terminal, zero submit.
8. waits then account risk fails -> terminal/blocked, zero submit.
9. waits then remaining R becomes poor -> terminal or continued hold according to reviewed policy, never forced submit.
10. direction reversal -> current #421/current-main rearm semantics.
11. duplicate worker callbacks -> one owner.
12. restart during WAIT -> same durable continuation restored.
13. restart after READY but before selector -> one owner, no duplicate materialization.
14. stale quote -> no READY promotion.
15. 5m data unavailable -> bounded wait/explicit unavailable disposition, no fake confirmation.
16. DB CAS unavailable -> zero broker submit.
17. session cutoff reached -> terminal/expire, zero submit.
18. PAPER and LIVE same market data -> same strategy classification, different rollout authority only.
19. PAPER state can never promote a LIVE order.
20. post-cancel #430 row is not consumed by this pre-submit lifecycle.
21. selector transient failure after READY -> existing selector retry authority, not a new efficiency retry loop.
22. stale selected contract after wait -> revalidate/reselect before broker post.
23. target/stop geometry uses underlying truth, not option premium.
24. trigger activation semantics preserve current scanner-stop policy.
25. no second broker POST after any durable accepted order exists.

## Expected files

Implementation should be surgical. Expected production scope:

1. one `entry_efficiency` policy/evaluator module
2. current breach/watcher handoff seam
3. existing durable pending-entry/rearm lifecycle module if metadata/CAS support is required
4. market-data helper only if PR 3 does not already provide dynamic 5m/15m refresh
5. tests + P0 workflow

Conditional sixth file only with explicit dependency explanation.

Do not edit:

- broker adapter
- exit engine
- proof logger
- scanner ranking
- core selector quality policy
- post-cancel retry owner except tests proving non-overlap
- position sizing thresholds

## Money-path audit questions

Final review must answer all:

- Does this change LIVE behavior? Flag dependent; initially observe-only.
- Is it active by default? **NO** for LIVE.
- Does it add broker submit/cancel calls? **NO**.
- Can it make existing submit seam reachable at a different time? Eventually yes under reviewed rollout; exact transition must be proven.
- Does it mutate orders? Only existing pending-entry lifecycle metadata/state through exact CAS if implementation requires it.
- Does it mutate positions? **NO**.
- Does it mutate `proof_trades`? **NO**.
- Does it mutate queue eligibility? **NO**.
- Does it preserve `client_id` / `execution_mode`? **YES, exact/fail closed**.
- Could it make Jason trade stale junk? Required answer **NO**, because every delayed promotion revalidates dynamic market/risk/final-submit truth.
- Could it miss winners by waiting? **YES, potentially**, which is why LIVE authority requires measured promotion evidence rather than intuition.

## Definition of done

We can distinguish “good setup, bad immediate entry” from “bad setup,” preserve ownership while waiting, and later enter only if fresh market truth still proves the opportunity.

## Release verdict

Current state: **HARD HOLD — docs only.**

Implement observe-only first. PAPER authority comes only after replay. LIVE authority comes only after exact outcome evidence and explicit approval.