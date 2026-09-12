# PR #618 — Pure Intraday Setup Classifier Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC ONLY. DO NOT MERGE OR DEPLOY.**

Depends on #617's canonical bar-state contract.

## Purpose

Provide pure deterministic classification over completed bars plus one forming bar. No data acquisition or orchestration lives here.

## Input contract

Inputs must be ordinary immutable bar-state values supplied by #617 or fixtures:
- timeframe;
- previous completed bars;
- current forming bar;
- session/bar identity;
- explicit timestamps.

No function in this module may fetch quotes, candles, DB rows, broker data, current time, or environment-dependent market state.

## Primitive bar classification

Relative to the immediately prior completed reference bar:
- `1`: current high <= prior high AND current low >= prior low;
- `2U`: current high > prior high AND current low >= prior low;
- `2D`: current low < prior low AND current high <= prior high;
- `3`: current high > prior high AND current low < prior low.

Equality policy must be explicit and tested. Do not introduce fuzzy/tolerance comparisons unless a separate spec authorizes it.

A developing forming bar may change state as new extremes arrive. Example:
`1 -> 2U -> 3`.

Once both sides have broken it is `3` for that bar. It must not remain a 2-side candidate because one side broke first.

## Initial setup family: 2-3-2

Detect both developing and completed forms.

Example directional interpretation:
- prior sequence `2U -> 3` + current developing `2D` => developing bearish 2-3-2;
- prior sequence `2D -> 3` + current developing `2U` => developing bullish 2-3-2.

The classifier returns structure only. It does not decide whether the setup is good enough to trade.

Recommended output fields:
- `setup_family`;
- `timeframe`;
- `direction`;
- `bar1_type`;
- `bar2_type`;
- `bar3_type`;
- `developing`;
- `completed`;
- `trigger_reference` if structurally derivable;
- `invalidation_reference` if structurally derivable;
- `reason_codes`;
- `classifier_version`.

Do not include client, account, contract, broker, quantity or execution policy here.

## Required tests

At minimum:
1. pure 1;
2. pure 2U;
3. pure 2D;
4. pure 3;
5. exact-boundary equality cases;
6. forming 1 -> 2U;
7. forming 2U -> 3;
8. forming 1 -> 2D;
9. forming 2D -> 3;
10. bullish developing 2-3-2;
11. bearish developing 2-3-2;
12. completed bullish 2-3-2;
13. completed bearish 2-3-2;
14. 2-3-3 is not 2-3-2;
15. wrong sequence is no setup;
16. malformed/nonfinite OHLC fails closed;
17. input objects are not mutated;
18. deterministic replay of identical inputs gives byte-equivalent canonical output.

## Explicit non-scope

No:
- market-data fetch;
- forming-bar construction;
- scanner scheduling;
- FVG/VI logic;
- watcher;
- selector;
- risk/sizing;
- broker;
- order/position/proof/queue mutation.

## Merge gate

Implementation must remain small and pure. Require focused tests, exact-head P0/cohesion enrollment if appropriate, merge-ref parity, `git diff --check`, and an independent scope audit before merge clearance.
