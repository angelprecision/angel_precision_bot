# P0 PAPER Sandbox Rescue Priority

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One PAPER-only defect

A correctly marketable PAPER ENTRY can remain unfilled in Tradier sandbox. Generic MISSED_MOVE cancellation may currently take ownership before the existing PAPER reprice/rescue window can run.

## Stack

This is PAPER-only and does not block LIVE stabilization.

Required order: `#440 terminal proof -> this PR -> #475 canonical retry`.

## Required implementation

Expected production scope: `ap/order_monitor.py` only.

1. For exact PAPER broker-open ENTRY ownership, existing PAPER rescue/reprice authority gets priority before generic MISSED_MOVE cancellation.
2. Every cancel/replace remains fenced by #440 exact terminal truth.
3. LIVE behavior is unchanged.
4. Do not create a second repricer or retry engine.
5. Preserve exact client, paper mode, local order, broker order, OCC, quantity, and generation identity.

## Required proof

- PAPER order at rescue window reaches existing reprice/fallback path before generic MISSED_MOVE.
- LIVE path byte-for-byte/behaviorally unchanged.
- old broker order OPEN/UNKNOWN/partial/full means zero duplicate replacement.
- duplicate ticks/restart create at most one generation.
- no selector, score, sizing, risk threshold, queue, proof, exit, or intelligence change.