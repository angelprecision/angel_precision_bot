# PR #617 — Deterministic Intraday Forming-Bar State Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC ONLY. DO NOT MERGE OR DEPLOY.**

## Purpose

Maintain exact, deterministic in-session forming/completed OHLC state for a bounded highly liquid ticker universe and session-anchored intraday timeframes.

Initial universe examples:
`SPY, QQQ, IWM, AAPL, GOOGL, NVDA, MSFT`

Initial timeframes:
- 5m
- 15m
- 30m
- 60m

## Ownership

#617 owns only bar-state construction and lifecycle.

It does not classify trading setups and does not emit trading candidates.

## Session anchoring

Use US-equity RTH session anchoring from 09:30 ET.

Examples:
- 60m buckets: 09:30–10:30, 10:30–11:30, 11:30–12:30, etc., with final shortened session bucket handled explicitly;
- 30m buckets: 09:30–10:00, 10:00–10:30, etc.;
- 15m/5m equivalent RTH anchoring.

Premarket/postmarket must not silently enter RTH bars.

Half days/early closes must be explicit rather than guessed.

## Incremental state

Seed required history once through the approved market-data source.

After seeding, accepted updates mutate only bounded current state:
- open remains first accepted observation for bucket;
- high = max(existing high, new price/high);
- low = min(existing low, new price/low);
- close/last = latest accepted observation;
- volume handling must use one explicit source/aggregation contract.

Do not refetch full history for every update or every timeframe.

## Completion semantics

A bar is `FORMING` until its exact session-anchored bucket end.

At boundary:
- finalize once;
- append/promote to completed state exactly once;
- open a new forming bucket if session remains open.

Late/out-of-order data policy must be explicit. Do not let a late update silently rewrite already-frozen completed decision-time evidence without a versioned correction mechanism.

## State identity

Each bar state should bind:
- ticker;
- timeframe;
- trading/session date;
- bucket start;
- bucket end;
- forming/completed status;
- source/version;
- last accepted source timestamp.

## Restart/reconstruction

Restart must reconstruct the same current/completed bar state from exact accepted source history/state, not from wall-clock guesses.

Do not produce duplicate completed buckets after restart.

## Data-quality behavior

Malformed/nonfinite prices, invalid timestamps, wrong-session data, impossible OHLC, duplicate updates and out-of-order updates must have deterministic outcomes and diagnostics.

Unknown/bad input must not fabricate a valid bar.

## Required tests

At minimum:
1. 5m bar opens/updates/finalizes correctly;
2. 15m equivalent;
3. 30m equivalent;
4. 60m 09:30 anchor correct;
5. 60m current bar develops intrahour without waiting for close;
6. boundary finalization occurs once;
7. next bucket opens correctly;
8. premarket excluded;
9. postmarket excluded;
10. weekend excluded;
11. malformed/nonfinite update ignored/fails closed;
12. duplicate update idempotent;
13. out-of-order update follows explicit policy;
14. restart reconstructs identical bar identity/OHLC;
15. multiple tickers remain isolated;
16. same ticker multiple timeframes remain isolated;
17. bounded universe enforcement;
18. no setup classification occurs;
19. no watcher/selector/broker/order/position/proof/queue mutation;
20. no repeated full-history fetch per accepted update.

## Explicit non-scope

No:
- STRAT/setup classification;
- FVG/VI interpretation;
- candidate scanner;
- signal promotion;
- watcher;
- selector;
- risk/sizing;
- broker;
- order/position/proof/queue mutation.

## Merge gate

Require focused state-machine tests, transport/caching audit, exact-head + merge-ref P0/cohesion where enrolled, `git diff --check`, bounded scope audit, and proof that update complexity is incremental rather than history-rebuild based.
