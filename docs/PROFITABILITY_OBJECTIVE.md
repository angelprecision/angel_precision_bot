# Profitability objective and evidence contract

The target is a daily, per-account selection policy that chooses at most the
best 5-7 eligible opportunities from the scanner stream and aims for:

- observed win rate of at least 80%;
- average losing trade no worse than 12%;
- average winning trade of at least 18%, with 18-25% as the reference band;
- positive per-trade expectancy.

These are engineering targets, not guaranteed returns. A policy is not allowed
to claim them from the same data used to tune it.

## Non-negotiable evidence boundary

The ranking policy must be frozen at a timezone-aware timestamp. Only decisions
made after that timestamp count as holdout evidence. Every candidate must carry
a score stamped before its outcome is known. Ranking features cannot contain
realized P&L, future returns, exit price/reason, MFE/MAE, win flags, target/stop
hits, or counterfactual resolution fields.

Outcomes are stored in a separate object and must have a `known_at` timestamp
strictly later than the decision timestamp. Missing data is unresolved, never a
synthetic win, loss, or zero.

## Daily selection rule

`ap.profitability_objective.select_frozen_policy_candidates()` groups eligible
candidates by `(client_id, session_date)`, ranks them by the frozen
`policy_score`, and selects no more than seven. Ties are deterministic. Outcomes
are never read during selection. If fewer than five candidates clear the
policy, the evaluator reports an underfilled session; it does not fill a quota
with weak trades.

The report preserves both `source_opportunities` and `eligible_opportunities`
so the operator can compare the evaluated population with the scanner's daily
source count and detect a hand-picked or incomplete export.

The bot should continue capturing rejected and unselected candidates through
the existing counterfactual and dossier paths. The 5-7 cap controls client
exposure, not research visibility.

## Promotion verdicts

- `NOT_ENOUGH_DATA`: fewer than 20 holdout account-sessions or 100 resolved
  selected trades.
- `HOLD_DATA_QUALITY`: fewer than 95% of selected trades have resolved outcomes.
- `HOLD_TARGET_MISSED`: enough data, but at least one observed target misses.
- `HOLD_UNPROVEN`: observed targets pass, but 95% confidence bounds do not.
- `PAPER_PROMOTION_CANDIDATE`: point targets and conservative confidence bounds
  pass. This is permission for reviewed paper testing only, never automatic live
  enablement.

An observed 80% win rate over 100 trades normally remains `HOLD_UNPROVEN`
because its lower confidence bound is below 80%. This is intentional.

## JSONL input

One line per scanner opportunity:

```json
{
  "opportunity_id": "canonical-id",
  "client_id": "paper-account",
  "session_date": "2026-07-14",
  "observed_at": "2026-07-14T13:42:00+00:00",
  "policy_version": "ap-ranker-v1",
  "policy_score": 84.5,
  "eligible": true,
  "features": {
    "scanner_score": 78,
    "position_profile_score": 86,
    "market_alignment": "ALIGNED",
    "spread_pct": 0.08
  },
  "outcome": {
    "return_pct": 0.21,
    "known_at": "2026-07-14T19:55:00+00:00"
  }
}
```

Run the evaluator:

```bash
python3 scripts/profitability_objective_report.py \
  --source holdout_opportunities.jsonl \
  --policy-version ap-ranker-v1 \
  --policy-frozen-at 2026-07-13T20:00:00+00:00
```

No code in this contract changes admission, watcher ownership, contract
selection, order submission, position sizing, stops, targets, or exits.
