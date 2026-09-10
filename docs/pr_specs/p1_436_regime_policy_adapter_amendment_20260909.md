# PR #436 Amendment: Deterministic Regime/Pullback Policy Adapter

## Status

**DRAFT / HARD HOLD. PAPER-TARGETED AUTHORITY ONLY WHERE ALREADY EXPLICITLY ENABLED. LIVE REMAINS OBSERVE-ONLY. DO NOT MERGE OR DEPLOY.**

PR #436 is the deterministic action/policy seam that follows the frozen intelligence base in PR #435.

The architecture is:

`#435 frozen BREACH evidence -> #436 deterministic policy evaluation -> existing watcher/selector/final gates/OSM -> broker`

PR #436 does not become a model-driven execution engine. #435 may describe regime, pullback state and setup archetype. #436 decides whether the existing opportunity should continue now, wait under one durable owner, rearm under the existing lifecycle, or terminalize for an already-authoritative deterministic reason.

## Relationship to #435

#435 is the canonical observation layer and owns versioned fields such as:

- `regime_pullback.schema_version`;
- `regime`;
- `pullback_state`;
- `setup_archetype`;
- `entry_posture_observe_only`;
- 4H/1H FVG relationship;
- first/second-touch lineage;
- 5m/15m point-in-time confirmation;
- remaining R / move consumed;
- exact client/mode/signal/order/lifecycle identity.

These fields are **advisory evidence** to #436 until exact outcome attribution and promotion evidence prove a particular policy. A #435 output, score, posture or confidence can never independently create `READY_NOW` or broker eligibility.

## Core invariant

The intended behavior is:

> Preserve a still-valid trade thesis through a bad immediate entry window, but never preserve stale or invalid money-path authority.

A valid setup may be temporarily inefficient. A temporary pullback must not be confused with permanent setup invalidation. Conversely, a classifier saying `TREND_PULLBACK` is not permission to buy into stale geometry, completed targets, broken stops, insufficient capital, invalid contracts, or contradictory lifecycle identity.

## Deterministic decision contract

#436 continues to resolve to the existing four conceptual outcomes:

- `READY_NOW`
- `WAIT_CONFIRMATION`
- `REARM_FOR_REBREACH`
- `TERMINAL_INVALID`

The exact public status representation must continue to respect existing OSM/watcher enums. Do not invent a second lifecycle.

### READY_NOW

`READY_NOW` requires fresh direct market truth at evaluation time. It may not be produced solely from a frozen #435 classification.

At minimum revalidate:

- exact client/mode/canonical signal/local order/generation ownership;
- current quote freshness;
- current relation to trigger;
- stop/target geometry;
- target not already completed;
- remaining opportunity still positive;
- required 5m/15m direct confirmation for the targeted policy;
- current FVG/structure state if that state is part of the policy;
- current account/risk/capital/final-submit gates;
- no conflicting active broker/order/position state.

### WAIT_CONFIRMATION

Use when the setup remains valid but immediate entry evidence is insufficient. Examples include:

- first touch inside an active pullback;
- wick-only breach;
- price returns to the pre-trigger side after first breach;
- first FVG touch without reclaim;
- second-touch/reclaim evidence not yet present for a policy that requires it;
- current quote/candle evidence is temporarily unavailable but opportunity has not deterministically expired.

WAIT retains exactly one durable owner. It must not create a second watcher, second order, second retry loop, or second broker submit path.

### REARM_FOR_REBREACH

Use only through existing canonical rearm semantics when the first breach has failed but the setup itself remains valid for another genuine trigger crossing.

A `SECOND_TOUCH` or `RECLAIMING` classification from #435 is evidence, not proof that rearm occurred. The durable runtime transition must prove the actual reset and re-breach.

### TERMINAL_INVALID

Terminal only for deterministic current truth such as:

- target complete;
- invalid/broken setup geometry;
- session/opportunity deadline expired;
- current stop invalidation;
- current risk/capital rejection;
- identity/generation contradiction;
- existing canonical terminal policy.

Do not terminalize merely because #435 emits `REJECT_CANDIDATE` or low confidence. #435 remains observe-only.

## Pullback / touch policy research contract

Do not hard-code `second touch = enter` globally.

The first policies to evaluate should be explicit and narrow, for example:

- first-touch FVG retest -> WAIT pending reclaim;
- active pullback -> WAIT pending completed reclaim/follow-through;
- genuine reset followed by second breach -> candidate for READY only after fresh direct validation;
- deep pullback with broken structure -> terminal or rearm according to existing deterministic invalidation policy;
- no pullback + strong completed continuation + healthy remaining opportunity -> candidate for READY.

Each policy must have a version and an explicit cohort. No hidden prompt or model output may choose policy semantics.

## Current rollout boundary

Preserve the existing #436 rollout contract:

- unset/malformed -> `observe_only`;
- targeted `paper_authoritative` remains limited to the already-defined canonical daily 2-3-2 cohort;
- LIVE remains observe-only in this PR;
- no environment combination may silently turn LIVE authoritative.

This amendment does not broaden PAPER authority to every regime/archetype. New policy cohorts remain observe-only until separately evidenced and explicitly promoted.

## Mandatory QQQ / HOOD / LULU replays

Use the same exact production identities as the #435 replay amendment. For each incident, evaluate the state machine from first breach forward and report:

```text
frozen #435 regime/pullback/archetype
first-breach direct market truth
#436 decision at first breach
whether owner remained durable
first reset/reclaim/re-breach timestamp where exact truth exists
#436 decision at reevaluation
actual selector/submit reachability
broker ENTRY POST count
terminal/rearm reason if any
```

Do not claim a better hypothetical fill unless exact historical quote/contract evidence supports it.

## Failure-class tests

Add production-shaped behavioral tests for:

1. first touch -> WAIT, one owner, zero POST;
2. active pullback -> WAIT, one owner, zero POST;
3. completed reclaim with fresh direct truth -> one READY transition;
4. second genuine breach after reset -> one READY transition maximum;
5. classifier says READY candidate but current target complete -> terminal/zero POST;
6. classifier says WAIT but current hard invalidation occurs -> terminal/zero POST;
7. #435 snapshot missing -> no favorable authority;
8. #435 snapshot stale -> no favorable authority;
9. #435 client/mode/canonical/local mismatch -> fail closed;
10. #435 schema version unknown -> advisory unavailable, not fallback favorable;
11. duplicate worker tick during WAIT -> same owner;
12. process death after WAIT persistence -> restart restores same owner;
13. process death after READY persistence before selector -> restart does not double promote;
14. ownership loss -> no mutation and zero broker POST;
15. DB CAS failure -> zero broker POST;
16. malformed pullback/FVG fields -> cannot produce READY;
17. direction reversal -> existing canonical rearm path only;
18. stale selected contract after wait -> selector revalidation/reselection, never stale submit;
19. accepted broker order already exists -> no second POST;
20. LIVE input under any current config -> #436 regime adapter cannot delay/allow/terminalize LIVE;
21. exact `client_id` / `execution_mode` preserved through WAIT/restart/rearm;
22. no cancel regression;
23. no position mutation before fill;
24. no `proof_trades` write before fill;
25. no queue/result corruption.

## Runtime / restart / materializer parity

For the same durable facts, uninterrupted runtime and restart recovery must resolve the same owner and the same deterministic policy state. #435 materialization timing must not change #436's broker authority.

A late-arriving #435 snapshot may enrich diagnostics. It may not retroactively authorize an already-blocked or terminal lifecycle generation.

## Money-path boundary

#436 may control timing only where the existing explicitly enabled PAPER policy already permits it. It must not introduce a new broker method, submitter, canceler or position/proof writer.

All broker submission remains through the existing final guarded path. All delayed promotions must still pass every existing selector, quality, affordability, risk, duplicate-submit and OSM guard.

## Current verdict

**HARD HOLD.** #436 is the fast deterministic policy layer after #435. Keep LIVE observe-only until #577/#443 produce exact outcome evidence and #438 explicitly clears a versioned policy for staged promotion.
