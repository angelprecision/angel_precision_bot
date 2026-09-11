# #435-B — BREACH market structure freeze

**Status:** DRAFT / OBSERVE-ONLY / HARD HOLD. Do not merge or deploy as entry authority.

## Purpose

Recut only the market-structure portion of #435 onto current `main`. This PR
does not inherit #435's 35-file branch. It freezes the structural facts needed
to evaluate later entry-timing policy without changing a single trading
decision.

Canonical FVG definition is the existing three-candle detector in
`ap/fair_value_gap.py`:

- bullish FVG: candle 1 high < candle 3 low;
- bearish FVG: candle 1 low > candle 3 high.

Canonical higher-timeframe geometry remains the existing RTH policy from
`ap/fvg_telemetry.py`:

- 4H: session-anchored 09:30 ET buckets `[09:30-13:30)` and `[13:30-16:00]`;
- 1H: session-anchored from 09:30 ET, final bucket shortened at the close.

No second FVG detector is introduced.

## Scope

Production:
- `ap/intelligence_breach_market_structure.py` only.

Tests:
- `tests/test_p0_435b_breach_market_structure.py`.

No existing money-path file is modified.

## Frozen evidence

`freeze_breach_market_structure()` produces a versioned, deterministic snapshot
containing:

- completed point-in-time 4H / 1H FVG zones;
- stable zone identity and lifecycle;
- price position relative to each gap: `inside | above | below | boundary`;
- nearest active opposing FVG and the far boundary that must be cleared;
- exact VI payload when supplied, otherwise explicit `MISSING`;
- 5m / 15m wick-versus-body penetration;
- `body_beyond_boundary_ratio`;
- `body_to_range_ratio`;
- observe-only `fifty_percent_body_through`;
- observe-only `strong_break_observed`;
- completed-close pullback / reclaim / re-breach sequence;
- supplied breach lineage preserved separately from inferred candle sequence;
- explicit upstream regime only, otherwise `UNKNOWN`;
- deterministic setup archetype and observe-only research posture.

## 50% FVG-break measurement

This PR records, but does not enforce, the operator rule discussed after the NOW
incident.

For a PUT crossing bullish FVG support, the break boundary is the FVG **low**.
For a CALL crossing bearish FVG resistance, the break boundary is the FVG
**high**.

A 5m or 15m bar records `strong_break_observed=true` only when all are true:

1. candle closes beyond the FVG boundary in trade direction;
2. the real body is directional;
3. at least 50% of the real body lies beyond the boundary;
4. the real body is at least 50% of the full candle range.

A wick through that closes back inside is `WICK_ONLY`.

These values are telemetry. They cannot allow, block, delay, rearm,
terminalize, or submit a trade in #435-B.

## Current production-data limitation

Current market-data infrastructure already sources 15-minute RTH bars and
aggregates them into 1H/4H FVG candles. Current `main` does not expose a
canonical exact 5-minute series to this new helper.

Therefore:

- 15m evidence can be consumed when supplied by the BREACH snapshot caller;
- 5m remains `MISSING` unless an exact point-in-time 5m source is supplied;
- this PR does not synthesize 5m candles from another timeframe;
- this PR adds no extra Tradier request.

Caller/wiring into the BREACH snapshot lifecycle is intentionally outside this
recut so the helper cannot silently become money-path authority.

## Authority and money-path invariants

The module accepts only mappings/scalars. It accepts no broker, selector, OSM,
order, position, proof-trade, queue or persistence object.

It has:
- zero broker submit/cancel calls;
- zero order/position/proof/queue writes;
- zero client/execution-mode/signal identity mutation;
- zero admission authority;
- zero LIVE/PAPER branching;
- zero terminalization/rearm authority.

`observe_only=true` and `affected_eligibility=false` are stamped on the output.

## Point-in-time rules

- Missing `data_as_of` means no candle can be proven complete, so candle-derived
  evidence stays missing.
- Timestamp-less or malformed OHLC bars are excluded.
- Future/incomplete 4H/1H/15m/5m bars are excluded.
- Non-finite values cannot improve classification.
- VI is never approximated.

## Focused behavioral tests

The focused suite executes:

- inside / above / below / boundary FVG positioning;
- missing-as-of future-data exclusion;
- incomplete 4H invalidator exclusion;
- NOW-shaped PUT at bullish FVG bottom with wick-only rejection;
- strong PUT 15m >=50% body break;
- symmetric CALL break through bearish FVG resistance;
- completed-close pullback -> reclaim -> re-breach;
- exact-VI-or-MISSING contract;
- regime no-fabrication;
- malformed/non-finite candle failure;
- input immutability + zero-authority invariant.

Local isolated execution against the current canonical detector contract:
`11 passed`.

Repository CI remains the merge proof. This PR stays HARD HOLD until exact-head
CI is green and the diff is independently audited.

## Follow-up boundary

A later, separate entry-policy PR may consume this frozen evidence and implement
actual FVG admission rules. It must prove the rule against production replay
before any LIVE authority is enabled.

That later PR, not #435-B, owns decisions such as:
`WAIT_FVG_BREAK`, `REARM`, or `BROKER_READY`.
