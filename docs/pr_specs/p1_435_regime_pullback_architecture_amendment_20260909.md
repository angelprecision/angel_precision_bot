# PR #435 Amendment: Regime / Pullback Architecture as the Current Intelligence Base

## Status

**DRAFT / HARD HOLD / OBSERVE-ONLY. DO NOT MERGE OR DEPLOY AS LIVE AUTHORITY.**

PR #435 is the current intelligence base for Angel Precision. This amendment extends its existing canonical BREACH snapshot contract. It does not create a competing intelligence pipeline and it does not move execution authority into an AI/model layer.

The architecture is deliberately split:

`market truth -> deterministic feature extraction -> #435 frozen intelligence snapshot -> later deterministic entry policy -> existing selector / OSM / broker path`

The slow intelligence layer may describe context. The fast execution path remains deterministic.

## Why this amendment exists

Recent production behavior has repeatedly shown a distinction that the current entry path must learn to measure:

- the directional thesis can be correct;
- the first trigger breach can still be a poor immediate entry;
- price can pull back, defend structure, reclaim, and later continue;
- 4H/1H fair-value-gap location can materially change whether a first touch should be chased;
- a first touch, second touch, deep pullback and reclaim are not the same market state.

PR #435 already freezes breach geometry, remaining opportunity, HTF structure, FVG/path evidence, 15m/5m confirmation, VWAP/volume context, and breach lineage. This amendment makes the regime/pullback state explicit and versioned so later policy can consume a stable schema instead of inventing ad-hoc interpretations in the money path.

## Canonical responsibility of #435

#435 owns **observation and classification only** at the confirmed-breach seam.

It must answer, using only information available at or before the frozen breach timestamp:

1. What regime is the underlying in?
2. Is the current move impulse, pullback, stabilization, reclaim, or failure?
3. What structural setup archetype best describes the candidate?
4. Is the first breach structurally strong, marginal, late/extended, or occurring inside/into a structural obstacle?
5. What entry posture would the evidence suggest for research purposes?

It must **not** decide whether an actual LIVE order may submit.

## Required versioned output schema

Add a versioned `regime_pullback` namespace to each successful BREACH snapshot.

Minimum fields:

```json
{
  "schema_version": "regime_pullback_v1",
  "regime": "TREND_CONTINUATION | TREND_PULLBACK | RANGE | BREAKOUT | BREAKOUT_RETEST | MEAN_REVERSION | VOLATILITY_EXPANSION | EXHAUSTION | REVERSAL_RISK | UNKNOWN",
  "pullback_state": "NO_PULLBACK | PULLBACK_STARTING | PULLBACK_ACTIVE | PULLBACK_STABILIZING | RECLAIMING | PULLBACK_FAILED | UNKNOWN",
  "setup_archetype": "TREND_CONTINUATION | FVG_RETEST | BREAKOUT_RETEST | SECOND_TOUCH | DEEP_PULLBACK_RECLAIM | MOMENTUM_BREAKOUT | RANGE_REVERSAL | FAILED_BREAKDOWN | FAILED_BREAKOUT | UNKNOWN",
  "entry_posture_observe_only": "ENTER_NOW_CANDIDATE | WAIT_RECLAIM_CANDIDATE | WAIT_SECOND_TOUCH_CANDIDATE | REARM_CANDIDATE | REJECT_CANDIDATE | INSUFFICIENT_DATA",
  "confidence": null,
  "reason_codes": [],
  "evidence_complete": false
}
```

`confidence` is a bounded evidence score, not a win probability. Missing evidence must not be converted into favorable confidence.

`entry_posture_observe_only` is diagnostics only in #435. It must never gate, delay, allow, rearm, terminalize or submit an order.

## Timeframe model

Use actual completed candles for their actual timeframe. Do not synthesize 4H intelligence by pretending a pile of 15m observations is equivalent unless the existing canonical aggregation has exact completed-bar boundaries and point-in-time provenance.

Required structure state by timeframe where canonical data exists:

- 1D
- 4H
- 1H
- 15m
- 5m

Weekly/monthly context may remain descriptive but does not replace the trading timeframes above.

For every timeframe persist at least:

- trend / directional structure;
- last confirmed swing structure;
- impulse direction;
- current pullback depth;
- range expansion/contraction;
- distance to nearest relevant structure;
- FVG/VI relationship where that producer is canonical;
- source, source timestamp and age;
- `AVAILABLE | STALE | MISSING | ERROR` status.

A lower timeframe may refine a higher timeframe. It may not rewrite the higher-timeframe candle as though they were the same observation.

## FVG / structural-state contract

For each relevant 4H and 1H gap, preserve enough state to distinguish:

- approaching gap;
- first touch;
- second-or-later touch;
- inside gap;
- midpoint tested;
- deep penetration;
- defended/rejected;
- reclaimed;
- filled;
- broken/invalidated.

Minimum fields when source truth exists:

```text
fvg_id / stable identity
side
timeframe
top
bottom
midpoint
status
percent_filled
touch_count
first_touch_ts
last_touch_ts
penetration_pct
reclaim_state
rejection_state
distance_from_breach
distance_to_target_path
source / as_of
```

Do not infer `touch_count=1` from missing history. Missing history means unknown.

## Pullback-state features

The classifier must be deterministic and inspectable. At minimum calculate:

- distance from trigger at breach;
- percent of planned trigger-to-target move already consumed;
- retracement from most recent directional impulse;
- depth relative to the active 4H/1H structure or gap;
- whether price returned to the pre-trigger side after first breach;
- whether a completed 5m/15m bar reclaimed the trigger/structure;
- wick-only versus body-close breach;
- follow-through count;
- opposing impulse strength;
- elapsed time since first breach;
- first-touch versus subsequent-touch lineage.

Do not encode a universal `second touch = buy` rule. The purpose is to measure whether second-touch/reclaim behavior actually separates better entries.

## Regime -> archetype mapping

The mapping must be explicit code or explicit deterministic tables, never hidden prompt prose.

Examples of legal research mappings:

- bullish HTF + controlled pullback into aligned FVG + reclaim -> `TREND_PULLBACK / FVG_RETEST`;
- breakout + return to prior boundary + hold -> `BREAKOUT_RETEST`;
- strong directional expansion with little remaining reward -> `EXHAUSTION` or late-extension evidence, not automatic continuation;
- opposing 4H wall directly ahead -> obstacle reason code;
- range/chop + marginal trigger wick -> weak immediate-entry evidence.

These are classifier outputs, not LIVE admission rules.

## AI / model boundary

No LLM/model call is allowed in the breach-to-selector hot path.

If a future model is used, it may only consume the already frozen structured #435 snapshot asynchronously and produce a separate advisory/explanation record. Model output must never directly:

- call broker APIs;
- create/cancel orders;
- mutate positions;
- write or finalize proof trades;
- change `client_id`, `execution_mode`, signal identity or lifecycle generation;
- manufacture missing market evidence;
- override deterministic hard safety gates.

No model output becomes authority merely because its confidence is high.

## Required production replays

In addition to existing AAPL evidence, the amendment must replay the recent September entry-timing incidents using exact production identities loaded from durable data at test/replay time rather than guessed ticker/time joins:

- QQQ: immediate entry became materially red before later recovery/continuation;
- HOOD: pullback before later recovery/continuation;
- LULU: pullback before later recovery/continuation.

For each, freeze the first-breach snapshot and report:

```text
regime
pullback_state
setup_archetype
entry_posture_observe_only
4H/1H FVG relationship
first/second-touch state
5m/15m confirmation
remaining_R / move_consumed_pct
actual entry timestamp
first subsequent reclaim timestamp where exact truth exists
```

Do not fabricate option prices for timestamps where no exact quote exists.

## Required behavioral tests

Add/retain production-shaped tests for at least:

1. bullish continuation with no pullback;
2. bullish setup with active pullback;
3. bearish symmetric pullback;
4. first FVG touch;
5. second touch after exact persisted first touch;
6. deep penetration then reclaim;
7. gap broken/invalidated;
8. wick-only trigger breach;
9. completed 5m reclaim;
10. completed 15m follow-through;
11. late extension with little remaining target distance;
12. opposing 4H wall directly in path;
13. missing 5m data -> explicit `MISSING`, never fabricated;
14. stale 4H/1H evidence -> explicit `STALE`;
15. future candle excluded;
16. malformed/non-finite candle value cannot improve classification;
17. first-touch history missing -> touch count `UNKNOWN`, not zero/one;
18. runtime and restart materializer resolve identical frozen classification from the same input;
19. duplicate enqueue remains idempotent;
20. classifier exception leaves existing execution path untouched;
21. observe-only `WAIT_*` posture cannot delay selector or broker path;
22. observe-only `REJECT_CANDIDATE` cannot terminalize a signal;
23. observe-only `ENTER_NOW_CANDIDATE` cannot make an otherwise blocked order submit;
24. exact `client_id` / `execution_mode` / canonical signal / local order remain unchanged;
25. zero broker submit/cancel, zero position mutation, zero proof mutation from this intelligence layer.

## Failure-class audit

Before #435 can leave HARD HOLD, independently audit:

### Authority
- timeframe source authority;
- breach/rebreach generation;
- exact client/mode/signal/order identity;
- FVG lifecycle identity;
- as-of timestamp authority.

### State transitions
- first breach;
- pullback after breach;
- reclaim;
- second touch;
- direction reversal/rearm;
- restart recovery.

### Failure timing
- snapshot handoff failure;
- DB persistence failure;
- worker death before/after claim;
- market-data source failure;
- restart between breach and materialization.

### Data corruption
- malformed candles;
- stale/future timestamps;
- conflicting duplicate identity;
- missing timeframe;
- invalid FVG geometry;
- invalid numeric data;
- legacy metadata disagreement.

### Money-path safety
Every failure path must prove:

- zero new broker calls;
- no order status mutation from classifier output;
- no position/proof/queue mutation;
- no LIVE/PAPER taxonomy crossing;
- no change to existing selector/risk/sizing/submit/cancel authority.

## Stack after this amendment

Use existing PRs instead of creating duplicate subsystems:

1. **#435**: canonical frozen market/structure/regime/pullback intelligence base, observe-only.
2. **#436**: bounded deterministic entry-efficiency lifecycle (`READY_NOW / WAIT / REARM / TERMINAL`) with current rollout restrictions. It is the policy seam, not #435.
3. **#437**: exact selected-contract intelligence after a real OCC contract exists.
4. **#577**: feature/outcome path capture needed to populate the edge dataset.
5. **#443**: exact attribution joining frozen intelligence to canonical trade outcomes without leakage.
6. **#438**: promotion gate. No LIVE intelligence timing authority until the evidence contract passes.
7. **#442**: later exit-target intelligence, separate from entry-timing work.

## Current verdict

**HARD HOLD.** This amendment expands what #435 records and classifies, not what Angel Precision is allowed to trade. The immediate production goal is to produce exact replayable evidence for pullback/FVG entry timing while keeping the existing money path unchanged.
