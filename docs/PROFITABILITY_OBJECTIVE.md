# Profitability objective and evidence contract

The target is a daily, per-account selection policy that presents at most the
best 10 eligible opportunities from the scanner stream, evaluates at most the
top seven, and aims for:

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

Score validity must cover at least 95% of the frozen source population, and
resolved outcomes must cover at least 95% of all eligible opportunities—not
only the selected trades. The report compares selected and unselected win rate
and expectancy, requires at least 100 resolved unselected controls, and records
score/return correlation. Conservative confidence bounds for both win-rate and
expectancy lift must remain positive. This proves ranking lift instead of merely
describing the trades the policy happened to choose.

The bot should continue capturing rejected and unselected candidates through
the existing counterfactual and dossier paths. The 5-7 cap controls client
exposure, not research visibility.

## Canonical intelligence score

The ranking input is now `ap.intelligence_score`, not the dossier's separate
case-quality grade and not the scanner's raw score. The score contract is:

- detailed position-profile output remains on its diagnostic 0-120 scale;
- `policy_score` is normalized to a strict 0-100 scale;
- `policy_score` is an ordering score, not a predicted win probability;
- the exact component set, each component maximum, profile arithmetic, and
  point-in-time freshness are validated before the score can rank;
- the formula, raw denominator, evidence threshold, and required components
  are versioned and hashed into `policy_version`;
- the coverage denominator is fixed across all ranking components, so missing
  components cannot improve coverage by shrinking the denominator;
- generic historical feedback remains visible as diagnostic context but is
  excluded from base score, coverage, and ranking because it is not guaranteed
  to have immutable point-in-time lineage;
- scanner quality, trigger geometry, and remaining opportunity must be
  available, and weighted component coverage must be at least 70%;
- invalid or under-covered scores remain in the source population with
  `eligible=false` and `policy_score=0` when converted for evaluation;
- block recommendations make a score ineligible for research ranking;
- every score carries account/mode/opportunity identity, `scored_at`,
  `data_as_of`, input hash, config hash, source, git commit, and an integrity
  hash over the final score envelope.

The queue's existing observe-only PRETRIGGER intelligence handoff creates this
score inside each durable intelligence snapshot. That is the population path
for measuring the scanner stream. Trade dossiers reuse the matching durable
score when it is already available; their fallback score is marked with its
own source and remains fail-closed if evidence is incomplete. None of these
fields participate in production admission or broker behavior.

## Connected daily feed and outcome ledger

`ap.intelligence_daily_rankings` freezes a deterministic selection after the
configured daily cutoff (10:30 America/New_York by default). It stores one
immutable ranking run per client, execution mode, session date, and policy.
The feed contains no more than 10 opportunities; only ranks 1-7 are marked as
the profitability evaluation cohort. Every selected row references the exact
PRETRIGGER snapshot and includes its score/input integrity hashes and dossier-
style intelligence review.

Apply both database migrations before enabling the feed:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/20260712_intelligence_context_snapshots.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f migrations/20260719_intelligence_daily_rankings.sql
```

Runtime settings:

- `INTELLIGENCE_CONTEXT_WORKER_ENABLED=1` (default) captures the source
  population;
- `INTELLIGENCE_DAILY_RANKING_ENABLED=1` (default) enables automatic freeze;
- `INTELLIGENCE_DAILY_FREEZE_ET=10:30` controls the daily cutoff;
- `INTELLIGENCE_DAILY_MIN_SOURCE_COUNT=10` prevents an early partial freeze,
  while `INTELLIGENCE_DAILY_HARD_FREEZE_ET=11:00` permits an underfilled day
  but still requires at least one captured source opportunity;
- `INTELLIGENCE_POLICY_FROZEN_AT` should be the reviewed policy release time.

Authenticated clients read `GET /intelligence/daily-feed` and
`GET /intelligence/daily-performance`. Admins can explicitly freeze or repair
outcomes through `POST /intelligence/admin/freeze-daily` and
`POST /intelligence/admin/reconcile-outcomes`.

Official performance rows are appended only by joining a selected LIVE signal
through its originating `orders.local_order_id` to a `proof_trades` row already
classified `LIVE_OFFICIAL` and `training_eligible=true`. PAPER and
counterfactual observations never enter the official target metrics.

## Promotion verdicts

- `NOT_ENOUGH_DATA`: fewer than 20 holdout account-sessions or 100 resolved
  selected trades.
- `HOLD_DATA_QUALITY`: selected or eligible outcome coverage is below 95%, or
  valid scores cover less than 95% of the frozen source population.
- `HOLD_TARGET_MISSED`: enough data, but at least one observed target misses.
- `HOLD_UNPROVEN`: observed targets pass, but confidence bounds or ranking-lift
  evidence do not.
- `PAPER_PROMOTION_CANDIDATE`: point targets, conservative confidence bounds,
  full-population data quality, and positive selected-vs-unselected ranking
  evidence pass. This is permission for reviewed paper testing only, never
  automatic live enablement.

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

The report command also accepts JSONL exports of intelligence snapshots or
trade dossiers directly. It detects their shape automatically, or the format
can be explicit:

```bash
python3 scripts/profitability_objective_report.py \
  --source pretrigger_snapshot_export.jsonl \
  --source-format intelligence-snapshot \
  --policy-version intelligence_policy_score_v2_observe_only:<config-hash> \
  --policy-frozen-at 2026-07-13T20:00:00+00:00
```

The snapshot row must contain either `intelligence_score` or
`payload.intelligence_score`. An outcome, when later known, stays in the
separate top-level `outcome` object.

No code in this contract changes admission, watcher ownership, contract
selection, order submission, position sizing, stops, targets, or exits.
