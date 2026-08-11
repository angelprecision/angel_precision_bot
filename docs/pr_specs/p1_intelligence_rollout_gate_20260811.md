# P1 Intelligence Profitability Promotion Gate

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is the final gate in the profitability-intelligence stack. It exists to prevent an observe-only score from becoming LIVE authority because a few recent trades looked good.

## Objective

Define the evidence required before any new BREACH or selected-contract intelligence changes Jason LIVE entry eligibility/timing.

## Required inputs

Promotion evaluation must consume only exact outcome-bound observations from the truth/outcome-binding PR. Unbound legacy rows, PAPER results, duplicated proofs, counterfactuals without exact point-in-time lineage, and rows with identity/mode conflict are excluded from LIVE promotion evidence.

## Required evaluation

For each candidate policy version report:

- total scanner/setup population
- number with complete intelligence profile
- number with missing/stale/error components
- actual executed cohort
- unselected/control cohort where valid
- sample count by LIVE/PAPER
- win rate
- average win
- average loss
- expectancy
- profit factor
- median return
- max adverse excursion where canonical
- max favorable excursion where canonical
- fill rate
- skipped-winner rate
- avoided-loser rate
- score decile performance
- monotonicity / ranking lift
- score-return correlation
- setup-score x contract-score matrix
- confidence intervals / uncertainty appropriate to sample size

No metric may be calculated from outcome-leaking feature recomputation.

## Promotion sequence

1. observe-only
2. PAPER authoritative
3. LIVE shadow recommendation with no behavior change
4. limited LIVE promotion only after explicit approval

No environment typo or generic `INTELLIGENCE_ENABLED=1` may skip stages.

## Safety constraints

Promotion tooling itself:

- does not call broker submit/cancel
- does not mutate orders/positions/proof trades/queue
- never rewrites execution mode
- never auto-tunes thresholds
- never auto-enables LIVE

A promotion artifact may produce only a candidate policy/config and evidence report. Actual LIVE enablement requires a separate reviewed config/deployment action.

## Fail conditions

Automatic HOLD if:

- exact outcome coverage is below reviewed minimum
- LIVE sample size is insufficient
- score does not show positive ranking lift
- selected high-score cohort expectancy is not positive
- average loss materially worsens without compensating expectancy
- skipped-winner rate is excessive
- result depends on one ticker/day/client
- policy was tuned and evaluated on the same unheld-out data without disclosure
- identity/schema health is degraded
- evidence contains PAPER/LIVE conflict

## Required tests

- unbound rows excluded
- PAPER excluded from LIVE metrics
- duplicate outcomes deduped by canonical identity
- conflicting mode rows quarantined
- future/outcome fields unavailable to scorer
- insufficient sample => HOLD
- negative expectancy => HOLD
- no ranking lift => HOLD
- strong in-sample but poor holdout => HOLD
- promotion cannot write LIVE config
- repeated report is deterministic for frozen inputs

## Definition of done

We have a mechanical reason to promote intelligence beyond “it feels better.”

## Release verdict

**HARD HOLD — docs only.**