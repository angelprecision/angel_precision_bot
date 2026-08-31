# P1 Affordable Contract Reselection

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One throughput defect

A setup may remain valid while the selected OCC contract becomes slightly unaffordable under final authoritative account budget. Rejecting that contract is correct; discarding the entire setup without one bounded cheaper-quality continuation can unnecessarily suppress trade flow.

## Invariant

`contract too expensive != signal invalid`.

When final authoritative cost rejects the exact selected contract:

1. Zero broker POST for that contract.
2. Invalidate only that candidate.
3. Perform bounded selector continuation with the authoritative max premium.
4. Keep every existing DTE/delta/spread/OI/volume/quote/risk threshold unchanged.
5. Never loop on the same unchanged unaffordable contract.
6. If a cheaper contract passes every existing gate, rerun all final submit gates and allow at most one broker POST.
7. If none exists, terminalize honestly.

## Stack position

Implement only after core entry reliability PRs #551, #548, #549, #550, #553, #554 are stable. PAPER #552 is independent of LIVE.

## Non-goals

No risk-cap increase, score/scanner/intelligence change, new broker cancel authority, position/proof/queue change, or selector-quality relaxation.

## Required proof

Replay the historical ~$169 selected / ~$166 final-budget class, cheaper-valid success, no-cheaper-contract terminal outcome, unchanged-candidate de-dup, restart/concurrency, malformed budget, LIVE/PAPER isolation, and exact broker call counts.