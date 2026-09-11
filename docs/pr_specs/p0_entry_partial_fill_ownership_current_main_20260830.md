# P0 ENTRY Partial-Fill Ownership — Current Main

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## Purpose

Old PR #480 asserted that a broker-confirmed partial ENTRY fill is already real exposure and must receive canonical position ownership immediately. Current `ap/fill_monitor.py` recognizes `PARTIAL_FILL` and advances OSM state, but this audit has not yet proven whether current main materializes/updates the canonical position at every partial fill.

## Rule

**Prove before editing.** If current main already satisfies the invariant, close this PR with behavioral evidence and zero production change.

## Required invariant

For BUY quantity 5:

1. Broker confirms cumulative filled quantity 2 while remainder 3 is still open.
2. Exactly one canonical position immediately owns quantity 2.
3. Exit monitoring may manage the real filled quantity without waiting for terminal ENTRY status.
4. Later cumulative fill 4 updates the same canonical position to quantity 4, never creates a second position.
5. Final fill 5 converges the same owner to quantity 5.
6. Restart at each boundary preserves one position owner and one broker ENTRY generation.
7. Cancel/retry logic consumes partial/late fill truth before any replacement BUY authority.
8. Exact client, `live|paper`, local order, broker order, OCC, signal, and position identity are preserved.

## If fail-first reproduces

Make the smallest fill-monitor/position-manager handoff correction required. Do not redesign fill monitoring or retry architecture.

No selector, score, sizing, risk threshold, scanner, exit policy, proof taxonomy, queue, or intelligence change. No new broker submit/cancel authority.

## Required proof

Behavioral tests for cumulative 2/5 -> 4/5 -> 5/5, restart after each step, late fill during cancel, duplicate monitor tick, LIVE/PAPER isolation, and exactly one canonical position.