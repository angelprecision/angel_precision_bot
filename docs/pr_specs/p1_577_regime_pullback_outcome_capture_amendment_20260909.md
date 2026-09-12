# PR #577 Amendment: Capture Regime/Pullback Outcomes for the Edge Dataset

## Status

**DRAFT / HARD HOLD / CAPTURE-ONLY. DO NOT MERGE OR DEPLOY UNTIL EXACT-HEAD EVIDENCE IS REVIEWED.**

PR #577 is the path/outcome capture layer for the intelligence stack. It must consume the frozen intelligence identity from #435 and the real selected-contract/path truth already available in the execution lifecycle. It must not create a competing intelligence classifier or execution owner.

Architecture:

`#435 frozen BREACH snapshot + #437 selected-contract snapshot + actual option/underlying path -> #577 append/update research outcome facts -> #443 exact attribution -> #438 promotion gate`

## Purpose

The immediate question is no longer merely whether a signal ultimately won or lost. We need to measure whether the **timing state at first breach** predicted entry quality.

The dataset must let us answer, without hindsight leakage:

- Did first-touch entries perform differently from exact second-touch/reclaim entries?
- Did `PULLBACK_ACTIVE` candidates suffer larger early MAE but later recover?
- Which 4H/1H FVG relationships produced favorable continuation?
- How long did trades take to become green after the first breach and after actual entry?
- How much of the planned move was already consumed at entry?
- Did waiting improve entry economics, or merely skip winners?

## Required frozen intelligence linkage

For every captured path/outcome row that originates from a #435-covered opportunity, persist immutable references to the exact frozen evidence used at decision time:

```text
breach_snapshot_id
breach_snapshot_input_hash
breach_snapshot_context_revision
regime_pullback_schema_version
policy/config/git lineage
client_id
execution_mode
signal_id
canonical_signal_id
local_order_id
lifecycle/breach generation where authoritative
```

Never copy a later/latest snapshot merely because it has the same ticker. The exact decision-time snapshot is required.

Persist the following #435 fields as denormalized research dimensions only when they are present in the exact bound snapshot:

```text
regime
pullback_state
setup_archetype
entry_posture_observe_only
4h_fvg_relationship
1h_fvg_relationship
fvg_touch_count / touch_state
5m_confirmation_state
15m_confirmation_state
remaining_R
move_consumed_pct
breach_timestamp
first_touch_timestamp
```

If any field is missing, persist missing/unknown. Never recompute historical structure from future candles to fill gaps.

## Required actual-path facts

Use exact observed data and existing feeds. Do not invent a second quote poller simply to make the dataset prettier.

Where exact truth exists, capture:

- actual ENTRY submit timestamp;
- actual ENTRY fill timestamp and fill price;
- selected OCC contract identity;
- option BID/ASK/mid at selector/submit when canonically available;
- first exact post-breach reset timestamp;
- first exact reclaim timestamp;
- first exact second-touch/re-breach timestamp;
- underlying price at those events;
- time from breach to actual fill;
- time from breach to first favorable excursion;
- time from actual fill to first green observation;
- maximum adverse excursion after actual fill;
- maximum favorable excursion after actual fill;
- peak/trough executable option BID where current path authority supports it;
- final canonical realized result only through the existing outcome/proof binding path;
- session cutoff / target / stop traversal timestamps where exact truth exists.

All timestamps require source/provenance and timezone-aware parsing.

## Counterfactual boundary

Do **not** manufacture a hypothetical delayed fill.

A delayed-entry counterfactual may be computed later only when all of these are true:

1. a shadow policy produced its decision prospectively at the original time;
2. the exact underlying/option contract path needed for the hypothetical is recorded from contemporaneous data;
3. quote provenance and executable-side economics are valid;
4. no future information is used to choose the delayed timestamp;
5. #443 marks it explicitly `COUNTERFACTUAL_RESEARCH`, never official trade outcome.

If those conditions are not met, the row may report actual path facts such as `first_reclaim_timestamp` but must not claim "the bot would have filled at X and made Y%."

## Metrics needed for #443/#438

The capture layer must make the following derivable without fuzzy joins:

```text
time_to_green_sec
breach_to_fill_sec
fill_to_mfe_sec
max_adverse_excursion_pct
max_favorable_excursion_pct
max_underlying_adverse_excursion
max_underlying_favorable_excursion
first_reclaim_delay_sec
first_rebreach_delay_sec
pullback_depth_after_breach_pct
actual_entry_vs_trigger_extension_pct
actual_entry_remaining_R
```

Use null when exact values are unavailable. Explicit zero and missing are not interchangeable.

## QQQ / HOOD / LULU replay contract

For the recent September incidents identified by #435/#436, load the exact durable trade identities and produce one joined research record per economic opportunity showing:

```text
#435 snapshot identity
regime / pullback_state / archetype
first-touch / FVG state
actual entry and fill
first exact adverse excursion
first exact reclaim/re-breach where available
time-to-green where exact
MFE / MAE where exact
final canonical outcome where available
```

Negative control: a same-ticker trade from another client/mode/day must never bind merely because timestamps are nearby.

## Production behavior boundary

The existing #577 implementation must remain fail-soft relative to trading:

- no selection/ranking change;
- no trigger/watcher timing change;
- no broker submit/cancel;
- no order-state authority;
- no position quantity mutation;
- no proof finalization authority;
- no queue eligibility mutation;
- no extra quote budget unless an existing feed explicitly exposes the same observation at zero additional transport cost.

Capture failure must be visible in diagnostics but cannot make a trade more or less eligible.

## Failure-class tests

Add production-shaped behavioral coverage for at least:

1. exact #435 snapshot binds by client/mode/canonical/local/generation;
2. wrong client cannot bind;
3. LIVE/PAPER mismatch cannot bind;
4. stale generation cannot bind;
5. latest unrelated snapshot cannot replace exact referenced snapshot;
6. missing snapshot remains missing, no ticker/time fallback;
7. malformed snapshot hash is rejected/quarantined;
8. explicit zero metric remains zero;
9. missing metric remains null;
10. timezone-naive event timestamp is unavailable, not silently localized;
11. option path reuse adds no broker/API call;
12. capture exception changes zero execution behavior;
13. duplicate path event is idempotent;
14. restart resumes capture without duplicate economic outcome;
15. partial fill path remains one economic opportunity;
16. same contract reused later cannot cross-bind;
17. first reclaim recorded only from exact observed event;
18. second touch recorded only when first-touch lineage is proven;
19. future candle cannot backfill #435 features;
20. actual realized result comes only from canonical outcome/proof authority;
21. counterfactual row cannot become LIVE_OFFICIAL;
22. PAPER research cannot become LIVE taxonomy;
23. no order mutation;
24. no position mutation;
25. no proof or queue mutation from capture path.

## Changed-line trace requirement

For every production file changed by #577, final review must trace:

`caller -> validation -> exact identity read -> capture mutation -> return classification -> downstream consumer`

Specifically prove that `ap_execution_core.py` instrumentation is downstream/side-band and cannot alter the existing decision returned to selector/OSM/broker code.

## Current verdict

**HARD HOLD.** #577 is required, but as measurement infrastructure. It must give #443/#438 trustworthy entry-timing evidence without becoming a hidden trading system of its own.
