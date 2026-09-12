# P1 — All-Trade 4H Structure Awareness

## Status

**DRAFT / HARD HOLD / OBSERVE-ONLY FIRST. NO LIVE SUPPRESSION AUTHORITY IN THIS PR.**

Base at creation: `main@71f25dabf739bce3360593ac44a7c7bf71ef9513`.

Dependencies:

1. canonical Massive 4H market-data source/alignment PR;
2. canonical 4H structure-map PR;
3. then-current #621/#622 durable/nonblocking intelligence seams.

## Purpose

Make every Angel Precision candidate cheaply aware of the already-computed 4H support/resistance map before entry evaluation, without fetching or rebuilding higher-timeframe data on the money path.

This PR attaches context. It does not decide WAIT/READY/REJECT and does not create new execution authority.

Canonical architecture:

`Massive 4H source -> precomputed 4H structure map -> local/cached lookup -> candidate 4H context`

`Tradier completed 5m/15m -> later acceptance/rejection/reclaim/re-breach interpretation`

## Absolute hot-path rule

No Massive/Polygon network call is allowed when a candidate arrives, a watcher triggers, selector runs, or broker execution begins.

No full 4H-history reconstruction is allowed in those paths.

No sleep/poll/wait for a structure refresh.

Runtime may only read an already-materialized local/shared structure map through a bounded lookup.

A map miss, stale map, malformed map or reader exception is `UNKNOWN` and existing behavior continues.

## Candidate scope

The awareness layer must be setup-family agnostic. It should work for:

- existing overnight opportunities;
- future added overnight scanners;
- intraday 2-3-2;
- intraday 3-2-2;
- 30m/60m structures;
- timeframe-continuity candidates;
- future FVG-generated bounce/rejection candidates;
- other candidate sources that use the canonical AP candidate envelope.

Do not require each scanner to fetch/calculate its own 4H map.

## Input identity

For one candidate, bind exact candidate identity already owned by AP, then look up one ticker-level 4H map version.

Preserve at minimum:

- candidate ticker;
- direction/side when known;
- candidate source/scanner family;
- candidate generation/version;
- decision/as-of timestamp;
- 4H map ID/version/generation/hash;
- map source data-as-of/freshness;
- HTF provider (`massive`);
- 4H alignment version;
- map calculation version.

Do not copy client identity into the shared map itself. Candidate-attached context may of course remain bound to the exact client/mode/signal identity through #625/#621.

## Output: `4H_CONTEXT`

Return/attach a deterministic context object conceptually containing:

- `status = COMPLETE | STALE | MISSING | ERROR | UNKNOWN`;
- map identity/version/generation/hash;
- map data-as-of/freshness;
- provider/alignment provenance;
- whether price is inside/intersecting a bullish FVG;
- whether price is inside/intersecting a bearish FVG;
- whether price is inside/intersecting a regular gap;
- whether price is inside/intersecting an ICT-style price/body imbalance;
- nearest support structure(s);
- nearest resistance structure(s);
- exact canonical structure IDs;
- boundaries;
- supplied-price distance in dollars/percent;
- fill/test/hold/break metadata from map where authoritative;
- reason codes.

The awareness object should retain exact structure IDs rather than only human-readable labels so replay/attribution can join deterministically.

## Price relation

Use the candidate's already-frozen/authorized underlying observation at the decision point when available. This PR must not fetch a new quote merely to calculate distance.

Given identical candidate observation + identical map, relation output must be deterministic.

Examples:

### Opposing support for PUT

`QQQ PUT candidate @ 602` + canonical bullish FVG `[600.80, 602.20]` -> `inside_bullish_fvg=true`, `direction_conflict=true`, exact FVG ID attached.

This PR records the relationship only. A later FVG policy decides whether authoritative 5m/15m evidence justifies WAIT.

### Aligned support for CALL

CALL candidate interacting with a bullish FVG -> aligned support context. Later #627 may use hold/reclaim confirmation to create/validate a bounce setup.

## Support/resistance relation semantics

Initial directional relation may expose pure labels such as:

- `ALIGNED_SUPPORT`;
- `ALIGNED_RESISTANCE`;
- `OPPOSING_SUPPORT`;
- `OPPOSING_RESISTANCE`;
- `NEUTRAL/NOT_NEAR`;
- `UNKNOWN`.

These labels are context only. They are not trade decisions.

Do not convert `OPPOSING_*` directly into rejection inside this PR.

## Multiple structures

A candidate may be inside or near more than one structure.

Return a bounded ordered set of relevant canonical structures, not one arbitrary winner.

Ordering must be deterministic, for example by direct intersection first, then distance, then canonical structure ID as stable tie-breaker. Exact ordering rule must be documented/tested.

Do not hide overlap between FVG/gap/imbalance types.

## Relationship to #614/#615/#621/#622

The eventual runtime path should become:

`candidate/confirmed breach -> cached 4H context + Tradier 5m/15m PIT evidence -> #615-style structure interpretation -> #625 envelope -> #621 durable snapshot -> #622 nonblocking runtime availability`

Exact wiring may be adjusted to then-current main, but ownership stays separated:

- Massive source PR owns HTF transport/normalization;
- 4H map PR owns shared structure calculation;
- this PR owns cheap candidate relation/attachment;
- #621 owns durable exact snapshot persistence;
- #622 owns nonblocking runtime handoff;
- later policy owns WAIT/READY;
- #627 owns aligned FVG hold/bounce classification;
- #628 owns eventual promotion.

Do not duplicate these owners.

## Source authority migration

Once this stack is promoted, canonical 4H geometry used for trade awareness must come from Massive-derived map identity, not from the legacy current `Tradier 15m -> locally aggregate 4H` path.

Keep Tradier 5m/15m lower-timeframe evidence initially.

Do not delete old telemetry/replay data merely because its source was legacy Tradier-derived 4H. Preserve source/version metadata so historical comparisons remain interpretable.

## Fail-soft behavior

Any of the following yields `UNKNOWN/MISSING/STALE` awareness and **must not block the current opportunity**:

- no map for ticker;
- map refresh failed;
- map stale under configured freshness policy;
- map identity malformed;
- current candidate observation missing;
- relation helper exception;
- source/provider metadata unproven;
- structure history coverage insufficient.

Log/telemeter the reason loudly. Do not manufacture `NO_STRUCTURE` from an unavailable map.

## Observe-only rollout

Initial implementation must set:

- `observe_only = true`;
- `affected_eligibility = false`.

Collect outcome/replay evidence comparing:

- candidates inside opposing 4H support/resistance;
- candidates aligned with 4H support/resistance;
- candidates far from HTF structure;
- later 5m/15m acceptance/rejection outcomes.

This PR alone may not alter selector reachability or broker execution.

## Performance contract

The local relation lookup must be cheap enough to apply to every candidate, including 25–50+ overnight opportunities per account, without multiplying provider work by clients.

Prove:

- same ticker/map reused across clients;
- no per-client provider fetch;
- bounded lookup time independent of raw history length after map construction;
- structure list is indexed/bounded sufficiently for runtime use;
- no synchronous refresh on miss.

Do not solve performance by dropping candidates silently.

## Required tests

At minimum:

1. PUT inside bullish 4H FVG -> opposing-support context only;
2. CALL inside bearish 4H FVG -> opposing-resistance context only;
3. CALL at bullish FVG -> aligned-support context;
4. PUT at bearish FVG -> aligned-resistance context;
5. price outside all structures -> neutral/not-near;
6. exact boundary equality;
7. overlapping structures retained;
8. nearest support/resistance deterministic order;
9. exact structure IDs preserved;
10. gap and FVG identities not conflated;
11. ICT price imbalance retained as distinct type;
12. same ticker across clients reuses same map;
13. same ticker CALL/PUT produces direction-specific relation without map duplication;
14. different ticker isolation;
15. stale map -> stale/unknown + existing behavior;
16. missing map -> missing/unknown + existing behavior;
17. malformed map -> error/unknown + existing behavior;
18. missing price -> unknown + existing behavior;
19. no quote fetch;
20. no Massive fetch;
21. no history reconstruction;
22. no sleep/poll/wait;
23. deterministic restart/replay;
24. source/alignment/map generation preserved into context;
25. observe_only true and affected_eligibility false;
26. zero watcher/selector/broker/order/position/proof mutation;
27. existing overnight/intraday throughput unchanged in control tests.

## Explicit non-scope

No market-data transport. No structure detection. No 5m/15m fetch. No FVG strong-break policy. No WAIT/READY. No bounce promotion. No selector/risk/sizing/broker/exit change. No broad #436 import.

## Merge gate

- canonical Massive source PR merged/cleared;
- canonical 4H structure-map PR merged/cleared;
- current #621/#622 ownership traced;
- runtime lookup proven local/nonblocking;
- observe-only/fail-soft tests;
- production-shaped multi-client throughput proof;
- exact-head P0/cohesion;
- genuine merge-ref parity;
- `git diff --check`;
- independent audit clears one unchanged final SHA.
