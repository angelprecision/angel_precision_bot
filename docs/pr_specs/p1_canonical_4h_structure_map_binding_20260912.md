# P1 — Canonical 4H Structure Map

## Status

**DRAFT / HARD HOLD / SPEC FIRST. NO TRADING AUTHORITY.**

Base at creation: `main@71f25dabf739bce3360593ac44a7c7bf71ef9513`.

Depends on the independently cleared canonical Massive 4H market-data source/preflight PR.

## Purpose

Transform canonical completed 4H bars into one deterministic, shared, replayable map of higher-timeframe support/resistance structures for each ticker.

This PR owns calculations and structure identity only. It does not fetch Massive directly and has no watcher/selector/broker authority.

Canonical path:

`Massive canonical completed 4H bars -> pure 4H structure map -> cache/persist once per ticker -> later all-trade awareness`

## Input contract

Consume only the normalized AP 4H bar contract from the canonical market-data PR.

Required input authority includes:

- ticker;
- exact completed 4H bars;
- source = Massive;
- source/alignment version;
- adjusted policy/version;
- exact bar IDs/start/completion timestamps;
- deterministic source-bar hash/generation;
- data-as-of/freshness metadata.

No network calls are permitted inside pure structure calculation.

Malformed, stale or identity-unproven input produces an unavailable/partial map with reason codes, never fabricated geometry.

## Structure families v1

### Fair Value Gaps

Detect deterministic three-candle FVG geometry on canonical completed 4H bars.

Persist at minimum:

- canonical structure ID;
- `type = BULLISH_FVG | BEARISH_FVG`;
- defining bar IDs/timestamps;
- lower/upper boundary;
- created_at;
- source timeframe = 4h;
- source/alignment version;
- initial geometry hash;
- fill state;
- first/last test metadata when derivable;
- break/hold state when derivable from completed canonical bars;
- invalidation reason/version.

The exact FVG mathematical definition must reuse or explicitly reconcile with Angel Precision's existing canonical FVG logic. Do not create a subtly different FVG definition merely because this PR has a new data source.

### Regular price gaps

Detect session/bar-to-bar price gaps under one explicit definition and version it.

Persist:

- bullish/bearish gap type;
- boundaries;
- defining bars;
- created_at;
- fill state;
- deterministic identity/hash;
- test/fill timestamps where known.

Do not mix regular price gaps with FVG identity.

### Volume imbalance v1 terminology

For this PR, **volume imbalance means ICT-style price/body imbalance derivable from OHLC geometry**, not bid-vs-ask executed-volume/order-flow imbalance.

Do not infer footprint/order-flow imbalance from OHLCV bars.

If future strategy requires true aggressor-side/order-flow imbalance, that requires trades/quotes/footprint reconstruction and must be a separate source/spec PR.

For v1 price/body imbalance, bind an exact formula before implementation and include direction, boundaries, defining bars and deterministic identity.

## Fill-state model

Structures should have explicit deterministic states rather than a single boolean when the geometry supports more nuance, for example:

- `UNFILLED`;
- `PARTIALLY_FILLED`;
- `FILLED`;
- `BROKEN/INVALIDATED` where applicable;
- `UNKNOWN` when history is insufficient.

The exact state machine must be defined by price interaction with immutable boundaries and completed bars. Do not rewrite original geometry when later price fills/tests it.

Original structure identity remains immutable; lifecycle state may evolve with a separate version/generation/update timestamp.

## Test/hold/break semantics

The map may record completed-4H interactions such as:

- number of zone tests;
- first_tested_at;
- last_tested_at;
- deepest penetration;
- completed-4H hold/break state.

This is higher-timeframe map telemetry. It does **not** replace the Tradier 5m/15m acceptance/rejection logic used at entry time.

Do not infer lower-timeframe acceptance from a 4H bar.

## Map identity

Create one deterministic map identity/version per ticker + source/alignment version + completed source boundary + structure-calculation version.

The same canonical input must produce byte-equivalent canonical output and the same map hash across restart/replay.

Changing only worker time, client/account, process ID or dict insertion order must not change map identity.

Changing actual completed source bars must change the appropriate source/map generation.

## Shared computation

The map is market structure, not client state.

Compute once per ticker/source generation and share across all clients/modes.

Do not create separate QQQ maps for Jason LIVE, PAPER, Jose, etc.

Required conceptual key:

`ticker + timeframe + provider/source version + alignment version + source generation`

Client/account identity belongs later when a candidate relates to this map.

## Persistence/cache contract

The implementation should support a cheap reader for runtime candidate awareness.

Persist/cache at minimum:

- map ID/version/generation/hash;
- ticker;
- timeframe;
- source + alignment version;
- source data-as-of;
- source bar identity hash;
- map computed_at as telemetry only;
- all active/relevant structures plus immutable defining geometry;
- lifecycle/fill/test metadata;
- status/freshness/reason codes.

Do not require a provider call to read the map.

A failed refresh does not erase last-good map. Last-good may be exposed as stale with explicit freshness metadata.

## Current-price distance

Distance from current price is candidate/runtime-relative and should not be baked into immutable structure identity.

The map may provide pure helper functions accepting a supplied price:

- nearest bullish support structure;
- nearest bearish resistance structure;
- price inside/below/above/intersecting structure;
- distance in dollars/percent;
- overlapping structure set.

Given identical map + supplied price, output must be deterministic. No quote fetch inside helper.

## Structure overlap

Multiple active structures may overlap. Do not collapse them into one anonymous zone unless a separate deterministic clustering rule is explicitly defined/versioned.

The reader should be able to return exact canonical structure IDs so later attribution knows which level affected a candidate.

## Unknown and insufficient history

A missing old structure because source history was not fetched is not proof the structure does not exist.

Map status must expose whether lookback is sufficient for the requested authority. If history is truncated, mark coverage/freshness explicitly.

Do not convert insufficient history into `NO_STRUCTURE` without a known complete coverage contract.

## No trading authority

This PR cannot:

- create/reject/delay candidates;
- create WAIT/READY;
- alter Master Control;
- install/terminalize watchers;
- call selector/broker;
- mutate risk/sizing/orders/positions/proof;
- fetch Tradier confirmation;
- promote an FVG bounce.

It answers only: **what major completed-4H structures exist for this ticker, according to the canonical Massive bar set?**

## Required tests

At minimum:

1. bullish 4H FVG exact geometry;
2. bearish 4H FVG exact geometry;
3. no-FVG control;
4. multiple distinct FVGs same ticker;
5. regular bullish/bearish gap exact geometry;
6. ICT-style bullish/bearish price imbalance exact geometry;
7. no accidental order-flow interpretation;
8. unfilled -> partial -> filled lifecycle;
9. completed-bar break/invalidation where defined;
10. structure geometry remains immutable after fill;
11. deterministic canonical IDs/hashes;
12. dict/order/restart invariance;
13. source-bar change changes map generation;
14. same ticker multi-client shares one map;
15. different ticker isolation;
16. alignment-version change isolates maps;
17. stale source status propagates without fabrication;
18. insufficient lookback exposed explicitly;
19. overlapping structures remain independently identifiable;
20. nearest support/resistance pure lookup from supplied price;
21. exact boundary/equality cases;
22. malformed/nonfinite source bars fail closed for map authority;
23. zero network calls;
24. zero trading/money-path mutation.

## Downstream read contract

Expose a bounded source-agnostic interface conceptually like:

`get_4h_structure_map(ticker, as_of=...) -> Canonical4HStructureMap`

and pure relation helpers.

Exact naming may differ. Downstream code must not know Massive transport details.

## Explicit non-scope

No provider adapter. No live/current quote acquisition. No 5m/15m logic. No all-trade enrichment. No FVG WAIT policy. No #627 bounce classification. No #628 promotion. No scanner expansion.

## Merge gate

- canonical Massive 4H source PR independently merged/cleared;
- exact FVG/gap/price-imbalance definitions bound;
- production-shaped replay fixtures;
- deterministic restart/idempotency proof;
- shared-per-ticker computation proof;
- focused tests;
- exact-head P0/cohesion where enrolled;
- genuine merge-ref parity;
- `git diff --check`;
- independent audit clears one unchanged final SHA.
