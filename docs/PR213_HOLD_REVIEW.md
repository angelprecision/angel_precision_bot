# PR 213 HOLD review checklist

This PR is open so the scoring-book process stays inside PRs 209 through 214.

Before merge, amend and verify:

- Use shared side normalization from PR 209A.
- Use thresholds from `ap/score_profile_config.py`.
- Return support and resistance stack diagnostics.
- Return stop protection diagnostics.
- Return target path diagnostics.
- Integrate `price_stacking` and `news_earnings_context` into `ap/position_score_profile.py` after PR 209A and PR 212 settle.
- Tests prove JSON-safe output, no input mutation, no external fetching, and diagnostics-only recommendations.
- Do not touch execution, queue state, order state, position state, proof records, score replacement, or sizing.

Verdict until amended: HOLD.
