# P0 ENTRY Cancel/Replace Terminal Proof

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One defect

Current ENTRY cancel/replace logic can treat successful cancel transport as sufficient authority to move toward replacement. Transport success is not proof the exact old BUY is terminal and incapable of late fill.

## Required invariant

Before any replacement/retry generation:

- OPEN/PENDING/WORKING -> zero replacement POST.
- UNKNOWN/query error/malformed -> zero replacement POST.
- PARTIAL/FILLED -> consume fill truth; zero duplicate full BUY.
- exact CANCELED/REJECTED/EXPIRED terminal truth -> replacement generation may become eligible.
- late fill beats cancel.
- at most one replacement generation.

Expected production owner: existing ENTRY cancel/replace seam in `ap/order_monitor.py`, reusing current canonical broker-status normalization.

## Dependencies

- #549 symbol-lock owner semantics.
- #550 partial-fill ownership proof/repair.
- PAPER only: this PR -> #552 -> canonical retry PR.
- LIVE does not wait on #552.

## Non-goals

No selector, score, sizing, risk threshold, scanner, exit, queue, proof, intelligence, or position-policy redesign.

## Required proof

Cancel ACK while broker still OPEN; UNKNOWN; partial fill; full late fill; exact terminal cancel; duplicate ticks; restart between cancel request and terminal proof; zero second BUY until old terminal truth is exact.