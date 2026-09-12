# PR #619 — Bounded Intraday Candidate Scanner Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC ONLY. DO NOT MERGE OR DEPLOY.**

Depends on:
- #617 canonical forming/completed bar state;
- #618 pure setup classifier.

## Purpose

Continuously evaluate a deliberately bounded highly liquid ticker universe and emit deterministic observe-only intraday setup candidates.

Initial universe examples:
`SPY, QQQ, IWM, AAPL, GOOGL, NVDA, MSFT`

Universe changes require explicit review. Do not silently expand to a broad market scanner.

## Architecture

`accepted #617 state update -> #618 classifier -> canonical candidate identity -> observe-only candidate`

No full-history refetch per candidate evaluation.

## Evaluation cadence

Allowed:
- reevaluate on each accepted canonical bar-state update; or
- bounded minute cadence using already-maintained state.

Forbidden:
- synchronous history fetch for each setup;
- per-setup duplicate quote/candle transport;
- sleeps in execution paths;
- coupling to broker submission.

## Candidate identity

Create deterministic canonical identity sufficient to dedupe one economic setup generation.

Identity should bind at minimum:
- ticker;
- timeframe;
- setup family/version;
- direction;
- session/trading date;
- defining bar identities;
- candidate generation/version.

Do not dedupe by ticker alone.

Same ticker opposite direction or different timeframe must remain independent.

## Candidate lifecycle

Required states should remain simple and observe-only, for example:
- `DEVELOPING`;
- `TRIGGERABLE`;
- `INVALIDATED`;
- `COMPLETED`;
- `EXPIRED`.

Exact names may differ, but behavior must be deterministic.

A developing candidate may update when the forming bar changes state. Example:
- developing 2-3-2 appears;
- current `2U` later breaks the opposite side and becomes `3`;
- prior 2-3-2 candidate must be invalidated/not emitted as still valid.

## Output contract

Candidate should contain structure only, for example:
- canonical candidate ID;
- ticker;
- timeframe;
- setup family;
- direction;
- defining bars;
- current classification;
- trigger/invalidation references if provided by classifier;
- first_seen_at from supplied state time;
- last_evaluated_at;
- classifier version;
- scanner version;
- `observe_only=true`.

No client account, OCC contract, quantity, broker-ready, eligibility or LIVE authority belongs here.

## Idempotency / restart

Given identical durable/reconstructed bar state, restart must rediscover the same canonical candidate ID rather than generate a new opportunity.

Required proofs:
- repeated same update emits once;
- restart reconstructs same ID;
- later state transition updates/invalidate same candidate generation appropriately;
- next session gets a new identity;
- same ticker multiple timeframes do not collide;
- opposite sides do not collide.

## Required tests

At minimum:
1. one SPY 60m 2-3-2 candidate emitted once;
2. duplicate evaluation emits no duplicate;
3. state change 2U -> 3 invalidates developing bullish 2-3-2;
4. bearish equivalent;
5. QQQ 60m and QQQ 15m remain independent;
6. CALL/PUT direction equivalents remain independent;
7. next session produces distinct candidate ID;
8. restart/replay produces same candidate ID;
9. malformed #617 state fails closed;
10. unknown classifier result emits no trade candidate;
11. classifier exception/failure is observable but no trading mutation occurs;
12. bounded universe excludes unconfigured symbol;
13. no network/DB/broker calls from pure evaluation seam unless persistence/telemetry is separately and explicitly added;
14. candidate output remains `observe_only=true`;
15. zero watcher/selector/broker/order/position/proof/queue mutation.

## Explicit non-scope

No:
- bar construction;
- setup math;
- FVG/VI intelligence;
- signal promotion;
- watcher install;
- selector;
- sizing/risk;
- broker calls;
- orders;
- positions;
- proof;
- trade_queue mutation.

Promotion into Angel Precision's existing signal/watcher pipeline is a separate future PR after #617-#619 are independently proven.

## Merge gate

Implementation must remain bounded, deterministic and observe-only. Require focused tests, exact-head and merge-ref parity, scope audit, no hidden market-data refetches, and `git diff --check` before clearance.
