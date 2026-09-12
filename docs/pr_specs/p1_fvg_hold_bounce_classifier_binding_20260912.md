# P1 FVG Hold/Bounce Classifier Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC FIRST. DO NOT MERGE OR DEPLOY.**

Base at creation:
`main@71f25dabf739bce3360593ac44a7c7bf71ef9513` (#625 merged).

## Stack position

`#614 PIT evidence -> #615 frozen FVG/market structure -> #621 durable BREACH adapter -> #622 nonblocking runtime bridge -> FVG hold/bounce classifier -> later promotion seam`

This PR owns **pure opportunity classification only**.

It is the offensive complement to the narrow defensive FVG WAIT/release policy:
- opposing FVG can delay an existing candidate;
- aligned FVG that demonstrably holds can create a new observe-only opportunity candidate.

Do not combine these two authorities into one policy surface.

## Purpose

Classify whether an authoritative 4H or 1H fair value gap is behaving as actionable support/resistance at decision time.

Initial target:

### Bullish bounce candidate
- relevant bullish 4H or 1H FVG exists;
- price retraces into, taps, or marginally penetrates the zone;
- authoritative completed 5m and/or 15m evidence is available;
- there is **no authoritative strong acceptance through the FVG low**;
- lower-timeframe evidence shows hold/rejection/reclaim consistent with support;
- emit an observe-only bullish/CALL setup classification.

### Bearish rejection candidate
- relevant bearish 4H or 1H FVG exists;
- price pushes into, taps, or marginally penetrates the zone;
- authoritative completed 5m and/or 15m evidence is available;
- there is **no authoritative strong acceptance through the FVG high**;
- lower-timeframe evidence shows hold/rejection/reclaim consistent with resistance;
- emit an observe-only bearish/PUT setup classification.

## Reuse existing intelligence

Do not rebuild market-structure interpretation.

Consume the canonical evidence already owned by #614/#615 and, after they exist, the durable/runtime seams owned by #621/#622, including where available:
- exact trigger/decision-time PIT evidence;
- 4H/1H FVG identity and geometry;
- relation of price to the zone;
- penetration measurements;
- wick vs real-body behavior;
- measured and authoritative body-through / strong-break state;
- 5m/15m source/coverage authority;
- reclaim/re-breach/path evidence;
- exact frozen observation/provenance;
- regime/setup context as telemetry only unless separately authorized.

No new market-data fetcher, FVG detector, snapshot system, worker, or execution subsystem.

## Authority rule

Positive trusted evidence may classify a bounce/rejection opportunity.

Unknown evidence is not positive evidence.

Missing, stale, malformed, provider-failed, non-authoritative, or identity-unproven intelligence must yield `UNKNOWN` / no new FVG-generated candidate.

This PR must not block, delay, reject, terminalize, or mutate any pre-existing trade opportunity because intelligence is unknown.

## Strong-break definition

Use the already-defined #615 authority. Do not create a competing threshold.

A strong break requires:
1. authoritative completed 5m or 15m candle;
2. close beyond the relevant FVG boundary;
3. directional real body;
4. at least 50% of real body beyond the boundary;
5. body at least 50% of total candle range.

If authoritative strong acceptance through the support/resistance boundary is present, the corresponding bounce/rejection setup is invalid.

## Suggested pure output

Return deterministic structure only, such as:
- `setup_family = FVG_HOLD_BOUNCE`;
- setup/version;
- ticker;
- timeframe of source FVG;
- direction (`BULLISH` / `BEARISH`);
- FVG identity and exact geometry;
- decision/evidence as-of timestamp;
- relation/penetration state;
- lower-timeframe authority used;
- hold/rejection/reclaim evidence;
- strong-break state;
- `state = CANDIDATE | INVALIDATED | UNKNOWN | NO_SETUP`;
- reason codes;
- deterministic input/output hash where existing intelligence identity supports it;
- `observe_only = true`;
- `affected_eligibility = false`.

No client account, OCC, quantity, selector result, broker-ready order, watcher ownership, or money-path authority belongs here.

## Explicit non-scope

No:
- WAIT/release authority for existing candidates;
- watcher creation;
- signal promotion;
- selector invocation;
- sizing/risk changes;
- broker calls;
- order/position/proof/queue mutation;
- second-touch requirement;
- generic pullback prediction;
- VI veto;
- regime veto;
- broad #436 timing-policy import;
- new scanner infrastructure unrelated to FVG opportunity classification.

## Required proof

At minimum:
1. bullish 4H FVG hold -> bullish observe-only candidate;
2. bearish 4H FVG hold -> bearish observe-only candidate;
3. symmetric 1H cases;
4. wick through boundary without authoritative strong break can still classify hold/reclaim when supporting evidence is present;
5. authoritative strong break through bullish FVG low invalidates CALL bounce candidate;
6. authoritative strong break through bearish FVG high invalidates PUT rejection candidate;
7. missing/stale/non-authoritative 5m/15m -> UNKNOWN, not candidate;
8. no future/current-market data leakage into frozen decision-time result;
9. exact ticker/side/FVG identity isolation;
10. deterministic replay from identical immutable evidence;
11. same ticker can independently carry distinct FVG opportunities when canonical FVG identity differs;
12. zero watcher/selector/broker/order/position/proof/queue mutation;
13. no changes to existing trade eligibility when this classifier fails or returns UNKNOWN.

## Merge gate

Do not implement production authority until #621 and #622 are independently cleared/merged and their exact contracts are known.

Implementation should remain pure and narrow. Require focused tests, #615 authority-compatibility tests, deterministic replay, exact-head P0/cohesion, genuine merge-ref parity, `git diff --check`, and an unchanged final SHA before clearance.

## Promotion dependency

A separate PR owns promotion of a proven FVG hold/bounce classification into Angel Precision's existing candidate/watcher pipeline. This classifier never submits or installs trades itself.
