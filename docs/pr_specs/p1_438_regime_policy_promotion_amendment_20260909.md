# PR #438 Amendment: Promotion Gate for Regime/Pullback Entry Policies

## Status

**DRAFT / HARD HOLD / GOVERNANCE ONLY. NO AUTOMATIC LIVE ENABLEMENT.**

PR #438 remains the final promotion lock for intelligence-driven timing authority.

Architecture:

`#435 frozen intelligence -> #436 versioned shadow/PAPER policy -> #577 path capture -> #443 exact attribution -> #438 evidence gate -> staged promotion only after explicit approval`

No model, score, classifier, dashboard or environment variable may bypass this gate.

## Why this amendment exists

The regime/pullback work creates tempting outputs such as:

- `TREND_PULLBACK`;
- `FVG_RETEST`;
- `WAIT_RECLAIM_CANDIDATE`;
- first-touch versus second-touch/re-breach states.

None of these deserves LIVE authority because several recent trades appeared to recover after pullbacks. Anecdotes are useful for generating hypotheses and terrible for setting production money-path policy.

#438 must prove that a specific versioned timing policy improves outcomes **without silently destroying fill rate or skipping too many winners**.

## Exact version binding

Every promotion report must bind at minimum:

```text
#435 regime_pullback schema version
#435 context/profile version
#436 policy version
#437 selected-contract evidence version where used
#577 capture version
#443 attribution version
code git SHA / config hash
promotion cohort start/end timestamps
```

Mixed policy/schema versions may be reported separately but cannot be pooled as though they were one policy.

## Required evidence population

Promotion evidence must come from exact #443 attribution only.

Exclude from the promotion denominator or quarantine explicitly:

- fuzzy ticker/time joins;
- unknown execution mode;
- client/mode conflicts;
- stale generation;
- duplicate/ambiguous proof;
- post-outcome reconstructed pretrade intelligence;
- hindsight-selected counterfactual timestamps;
- rows missing required decision-time policy identity.

The report must show excluded counts and reasons. Missing evidence cannot disappear from the denominator simply because it is inconvenient.

## Required regime/pullback slices

At minimum report separately:

- `NO_PULLBACK`;
- `PULLBACK_STARTING`;
- `PULLBACK_ACTIVE`;
- `PULLBACK_STABILIZING`;
- `RECLAIMING`;
- first FVG touch;
- proven second touch/re-breach;
- deep penetration + reclaim;
- late/extended breach;
- opposing 4H/1H wall;
- setup archetype;
- regime;
- opening-window versus later-session.

Each slice must include sample size and uncertainty. Do not promote a universal rule from one ticker behaving like itself.

## Baseline comparison

For every candidate policy compare against the actual current-entry baseline over the same eligible population.

Required outcomes:

```text
eligible opportunities
actual fills
shadow/PAPER candidate fills
fill-rate delta
win-rate delta
average-win delta
average-loss delta
expectancy delta
profit-factor delta
MFE delta
MAE delta
time-to-green delta
entry-extension delta
skipped-winner rate
avoided-loser rate
no-fill / expired-opportunity rate
```

`skipped-winner` and `avoided-loser` may be calculated only from valid prospective shadow-policy decisions with contemporaneous path evidence. Do not manufacture them from hindsight.

## Minimum promotion logic

Do not hard-code arbitrary numerical profitability thresholds in this amendment beyond existing organizational safety rules. The implementation must, however, automatically HOLD a candidate when any of the following is true:

- exact attribution coverage is materially incomplete;
- expectancy is non-positive;
- observed lift is absent or reverses out of sample;
- fill-rate degradation is unexplained or operationally unacceptable;
- skipped-winner cost overwhelms avoided-loser benefit;
- improvement is concentrated in one ticker, one day, one client, or one pattern;
- evidence is too small to distinguish signal from noise;
- PAPER/LIVE identity health is degraded;
- policy/schema versions are mixed or unproven;
- any new duplicate-submit, ownership, or taxonomy regression appears.

A high win rate alone is insufficient. A policy can achieve 100% win rate by refusing to trade. Humanity has discovered many similarly impressive ways to optimize the wrong metric.

## Promotion stages

Preserve the existing sequence:

`observe_only -> PAPER authoritative -> LIVE shadow -> explicitly approved limited LIVE`

Additional rules:

1. A regime/pullback classifier may begin observe-only immediately once its snapshot integrity is proven.
2. PAPER authority may be granted only to an explicit policy version/cohort.
3. LIVE shadow means the system records what the policy would have done but does not alter LIVE timing.
4. Limited LIVE requires explicit operator approval for a named policy version and bounded cohort.
5. Expansion requires another evidence review. No generic `ENABLE_INTELLIGENCE=true` flag may promote every policy.
6. Rollback must be immediate and must restore the prior deterministic execution behavior without requiring schema rollback.

## Required rollout safety proof

Before any limited LIVE promotion, execute behavioral tests proving:

- flag/config unset -> existing LIVE behavior;
- malformed config -> existing LIVE behavior / fail-safe classification;
- wrong policy version -> no LIVE authority;
- unknown #435 schema -> no LIVE authority;
- wrong client/mode -> no authority;
- stale generation -> no authority;
- ownership loss -> no mutation;
- current final-submit gate rejection still blocks;
- selected contract quality rejection still blocks;
- max positions/capital rejection still blocks;
- broker accepted-order duplicate fence still blocks a second POST;
- no new cancel path;
- no position mutation before fill;
- no proof write before fill;
- PAPER result cannot authorize LIVE;
- rollback returns to prior behavior on the next eligible lifecycle without orphaning WAIT/rearm ownership.

## Production acceptance monitor

Any future limited LIVE rollout must continuously report by exact policy version:

```text
eligible count
policy READY / WAIT / REARM / TERMINAL counts
actual fills
fill rate
broker POST count per generation
skipped opportunities
realized expectancy
win rate
avg win / avg loss
MFE / MAE
time-to-green
client/mode identity conflicts
ownership/CAS failures
rollback events
```

A safety or identity breach is an immediate HOLD regardless of short-term profitability.

## September incidents

QQQ, HOOD and LULU are mandatory replay fixtures because they motivated the pullback hypothesis. They are **not sufficient promotion evidence by themselves**.

The first goal is to see whether #435/#436 would have classified their timing correctly. The second is to accumulate enough shadow/PAPER observations to prove whether that behavior generalizes.

## Money-path authority

#438's tooling may generate reports and a promotion candidate decision. It may not directly:

- call broker submit/cancel;
- mutate orders/positions/proof/queue;
- change client/mode identity;
- auto-edit production environment variables;
- auto-enable LIVE policy.

A human-approved release change remains required for limited LIVE authority.

## Current verdict

**HARD HOLD.** No regime/pullback entry-timing policy is cleared for LIVE by this amendment. #438 exists specifically to stop "it looked good on three trades" from becoming production capital policy.
