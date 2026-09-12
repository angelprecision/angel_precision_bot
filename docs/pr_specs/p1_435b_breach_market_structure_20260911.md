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

## #614 point-in-time contract

Merged #614 now supplies the canonical BREACH envelope consumed by this PR:

- `point_in_time["as_of"]` is the exact `trigger_crossed_at` evidence boundary;
- `point_in_time["data_sources"]["candles"]` contains exact frozen 5m and 15m
  rows plus complete session-anchored 1H and 4H rows derived from 15m;
- `point_in_time["data_sources"]["coverage"]` and
  `point_in_time["provenance"]` travel with the snapshot;
- `point_in_time["underlying_observation"]` is the frozen breach observation.

`freeze_breach_market_structure_from_pit()` is a narrow adapter from that
envelope to the existing freezer. It uses `as_of`, never `collected_at`, and
does not refetch, aggregate, or synthesize candles. Exact 5m evidence is now
usable when #614 supplies it; a missing, stale, failed, or non-authoritative
source remains non-authoritative and cannot produce a trustworthy strong break.

Caller/wiring into entry authority remains intentionally outside this recut so
the helper cannot silently become money-path authority.

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

- Missing or invalid PIT `as_of` means no candle can be proven complete, so
  candle-derived evidence stays missing.
- `collected_at` is descriptive worker metadata only; it cannot move the
  evidence boundary.
- Timestamps must be timezone-aware. Naive or malformed timestamps are excluded;
  the helper never guesses that a naive value is ET.
- OHLC bars must start on the session-aligned 09:30 ET grid and fall inside
  US-equity RTH. Premarket, postmarket, weekend, and misaligned bars are
  excluded.
- Future/incomplete 4H/1H/15m/5m bars are excluded.
- Non-finite values cannot improve classification.
- The breach price comes only from explicit frozen breach evidence such as
  `breach_price`, `frozen_underlying_price`, or a timestamped breach-evidence
  mapping. Generic worker-time `underlying_price` / `current_price` aliases are
  not accepted.
- VI requires an available status, non-empty source, aware timestamp no later
  than the snapshot, and numeric exact evidence. Approximate or malformed VI is
  `MISSING`.
- Penetration evidence cannot predate the selected FVG's formation; pullback /
  reclaim / re-breach sequencing begins at the supplied breach timestamp.

## Focused behavioral tests

The focused suite executes:

- inside / above / below / boundary FVG positioning;
- missing-as-of future-data exclusion;
- strict timezone/session-bar admission;
- incomplete 4H invalidator exclusion;
- NOW-shaped PUT at bullish FVG bottom with wick-only rejection;
- frozen breach-price provenance and missing-price zone selection;
- strong PUT 15m >=50% body break;
- symmetric CALL break through bearish FVG resistance;
- pre-formation penetration exclusion;
- 5m/15m straddling-breach completion-boundary timestamps;
- real #614 BREACH envelope -> #615 adapter integration for exact 5m/15m and
  derived 1H/4H candles;
- frozen-observation precedence over contradictory current aliases;
- stale/provider-failure coverage cannot assert a strong break, while
  authoritative frozen coverage can measure one;
- completed-close pullback -> reclaim -> re-breach;
- exact-VI-or-MISSING provenance and approximation contract;
- regime no-fabrication;
- malformed/non-finite candle failure;
- input immutability + zero-authority invariant.

Local isolated execution against the current canonical detector contract:
`33 passed` after the #614 envelope adapter and completion-boundary amendment.

Repository CI remains the merge proof. Any exact-head and merge-ref checks from
the prior head must rerun for this amendment. This PR stays HARD HOLD.

## Follow-up boundary

A later, separate entry-policy PR may consume this frozen evidence and implement
actual FVG admission rules. It must prove the rule against production replay
before any LIVE authority is enabled.

That later PR, not #435-B, owns decisions such as:
`WAIT_FVG_BREAK`, `REARM`, or `BROKER_READY`.
