# PR #443 Amendment: Exact Regime/Pullback Outcome Attribution

## Status

**DRAFT / HARD HOLD / ATTRIBUTION ONLY. DO NOT MERGE OR DEPLOY AS TRADING AUTHORITY.**

PR #443 is the exact attribution layer for the current intelligence stack. It must join the frozen decision-time evidence from #435, selected-contract evidence from #437, actual path observations from #577, and canonical realized proof truth without fuzzy ticker/time inference or future-data reconstruction.

Architecture:

`#435 BREACH snapshot + #437 CONTRACT_SELECTED + #577 actual path -> #443 exact attribution -> #438 promotion report`

## Core invariant

Every attributed outcome must answer:

> Which exact frozen facts existed before the decision, what actually happened afterward, and which canonical economic trade owns the realized result?

It must never answer a different question by quietly substituting "the nearest QQQ trade at about the same time." Humans have tried that methodology before. It is called data contamination.

## Exact identity chain

Required authoritative joins must include the available exact identities across the producing records:

```text
client_id
execution_mode
signal_id
canonical_signal_id
local ENTRY order id
breach/lifecycle generation where authoritative
selected OCC contract identity
position/economic trade identity
canonical proof identity
```

A conflict or missing authority must produce `UNATTRIBUTABLE` / quarantine diagnostics, not a fallback join.

LIVE attribution requires exact LIVE evidence and revalidated official proof taxonomy. PAPER remains research-only. Unknown mode never promotes.

## Required pretrade dimensions from #435

Consume the exact referenced frozen snapshot, including when available:

- `regime_pullback.schema_version`;
- regime;
- pullback state;
- setup archetype;
- observe-only entry posture;
- 4H/1H FVG relationship;
- FVG touch count / touch state;
- 5m confirmation state;
- 15m confirmation state;
- breach timing/opening-window bucket;
- first breach vs re-breach lineage;
- remaining R;
- move consumed percent;
- breach/trigger extension;
- VWAP/volume/market/sector evidence with original provenance.

Never recompute these after the outcome. If the original snapshot says missing, attribution says missing.

## Required execution/contract dimensions

Consume exact #437 or canonical durable order/selector evidence for:

- OCC contract;
- DTE/strike;
- intended BUY ask/reference;
- spread;
- delta/gamma/theta/IV when point-in-time and canonical;
- OI/volume;
- quantity/debit;
- premium chase/expansion;
- selector rejection/candidate diagnostics.

Do not let post-fill option metadata masquerade as selection-time evidence.

## Required actual path dimensions from #577

Where exact observations exist, attribute:

- actual submit/fill timestamps;
- breach-to-fill delay;
- first reset timestamp;
- first reclaim timestamp;
- first proven re-breach/second-touch timestamp;
- time to first green observation;
- MFE/MAE;
- underlying favorable/adverse excursion;
- executable option BID peak/trough where canonical;
- session/target/stop traversal events.

Missing path data remains missing.

## Outcome truth

The official realized result must come only from the canonical proof/economic-trade authority. Attribution may not mutate proof rows or repair economics as a side effect.

Separate namespaces are required:

```text
pretrade
contract
execution
path
exit
outcome
counterfactual_research
```

This prevents future observations from leaking into the pretrade feature set.

## Required cohort analysis

#443 must produce attribution rows that allow #438 to measure at minimum:

- first touch vs proven second touch/re-breach;
- `NO_PULLBACK` vs `PULLBACK_ACTIVE` vs `RECLAIMING`;
- setup archetype;
- regime;
- 4H/1H FVG support/obstacle/touch state;
- 5m/15m confirmation state;
- remaining-R and move-consumed buckets;
- opening-window vs later-session breach;
- actual entry extension;
- time-to-green;
- MFE/MAE;
- realized win rate, average win/loss, expectancy and profit factor;
- fill rate and no-fill rate.

Results must be sliceable by ticker, date, client/mode and policy/schema version so concentration can be detected.

## Shadow-policy / counterfactual attribution

`avoided_loser`, `skipped_winner`, and delayed-entry improvement claims are permitted only when a versioned shadow policy generated its decision prospectively and #577 captured contemporaneous path evidence sufficient to evaluate it.

Required counterfactual labels:

```text
COUNTERFACTUAL_RESEARCH
policy_version
policy_decision_ts
candidate_action
execution_assumption_source
quote_provenance
```

A hindsight-selected reclaim timestamp is not a valid shadow-policy decision.

Counterfactuals can never be `LIVE_OFFICIAL`, canonical proof, or realized client performance.

## QQQ / HOOD / LULU replay contract

For each September incident referenced by #435/#436/#577, produce an exact attribution record showing:

```text
exact economic identity
#435 snapshot id/hash/schema
regime/pullback/archetype
FVG/touch state
5m/15m state
remaining opportunity
actual entry/fill
first exact reclaim/re-breach if captured
MFE/MAE/time-to-green if captured
canonical final outcome if available
shadow policy decision only if it actually existed prospectively
```

Required negative controls:

- same ticker, wrong client;
- same ticker, wrong execution mode;
- same contract reused by another trade;
- nearby timestamp but wrong canonical signal;
- stale generation;
- duplicate proof candidate;
- unknown/legacy mode.

All must fail closed to attribution.

## Failure-class test matrix

At minimum execute behavioral tests for:

1. exact identity chain attributes once;
2. wrong client -> unattributable;
3. mode conflict -> unattributable;
4. canonical signal conflict -> unattributable;
5. local order conflict -> unattributable;
6. stale generation -> unattributable;
7. duplicate candidate proof -> quarantine;
8. missing #435 snapshot -> explicit missing, no fuzzy fallback;
9. later #435 revision cannot rewrite older decision-time evidence;
10. future bar cannot populate pretrade namespace;
11. explicit zero remains zero;
12. malformed/non-finite metric remains invalid/missing;
13. missing path event remains null;
14. same OCC reused later cannot cross-bind;
15. partial fills remain one economic trade;
16. restart attribution is idempotent;
17. duplicate worker execution yields one attribution result;
18. PAPER result remains research-only;
19. unknown mode cannot become LIVE;
20. LIVE_OFFICIAL requires exact official proof revalidation;
21. negative P&L cannot be labeled win unless canonical proof semantics explicitly support it;
22. counterfactual cannot mutate official outcome;
23. counterfactual cannot be created from hindsight-selected timestamp;
24. attribution performs zero broker calls;
25. attribution performs zero order/position/queue/proof-economic mutations.

## Required report for #438

For every schema/policy version, report:

```text
eligible population
attribution coverage
unattributable/quarantined count and reasons
LIVE/PAPER split
first-touch vs second-touch sample counts
regime/pullback/archetype counts
win rate
average win
average loss
expectancy
profit factor
fill rate
time-to-green distribution
MFE/MAE distribution
skipped-winner / avoided-loser only from valid shadow cohorts
concentration by ticker/day/client
```

No policy may be promoted from a report whose exact-attribution coverage is materially incomplete or whose lift disappears outside one ticker/day.

## Money-path boundary

#443 has no permission to:

- submit/cancel broker orders;
- change eligibility;
- change watcher state;
- change selector behavior;
- mutate position quantity;
- rewrite proof economics;
- write queue authority;
- promote a policy to LIVE.

It may write only versioned attribution/research records required by the implementation contract.

## Current verdict

**HARD HOLD.** #443 is the truth-joining layer. It must make #438's profitability decision statistically and operationally defensible, not merely generate an attractive dashboard.
