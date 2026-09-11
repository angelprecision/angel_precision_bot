# P0 Canonical Post-Cancel ENTRY Retry

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One defect

`ap/order_monitor.py::_submit_armed_retry()` still imports legacy `ap.execution.process_signal`, so a legitimate retry can re-enter obsolete admission/time-gate behavior instead of the canonical ExecutionCore/OSM lifecycle.

## Required repair

After #553 proves the old broker ENTRY terminal and one durable retry generation is claimed:

1. Remove `ap.execution.process_signal` import/call from order-monitor retry.
2. Hand retry back to the current canonical ENTRY lifecycle.
3. Preserve exact client_id, `live|paper`, canonical signal, original local order, previous broker id, direction/OCC/breach lineage, retry root and attempt.
4. Rerun current duplicate, risk, setup, selector, quote, and final-submit gates.
5. At most one broker ENTRY POST.
6. No position/proof mutation before confirmed fill.

## Prerequisites

- #549 exact symbol-lock ownership.
- #550 partial-fill ownership proven/repaired.
- #553 old-order terminal proof.
- PAPER only: #552 before this PR. LIVE does not wait on #552.

## Required proof

- 09:31 PAPER single-stock retry does not inherit legacy pre-10 `ap.execution` gate.
- LIVE retry preserves LIVE identity.
- Duplicate ticks/restart produce one durable claim and at most one POST.
- Old order OPEN/UNKNOWN/partial/full late-fill means zero replacement POST.
- Static regression: `ap/order_monitor.py` contains zero `process_signal` import/call.

No selector threshold, score, sizing, scanner, exit, proof taxonomy, queue-policy, or intelligence changes.