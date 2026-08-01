# P0 Queue Liveness and Watch Ownership

> **DRAFT IMPLEMENTATION — UNMERGED AND UNDEPLOYED.**
>
> Base: committed GitHub `main` at `715d26ede22c6078c22cf5796836789332b5dfe5`
>
> This document records the implemented PR #404 contract. The PR body is the
> authoritative source for the exact head SHA and exact-head CI run. Merge and
> deployment remain separate operator decisions.

## Incident

The July 28 production funnel showed a large supply of approved candidates but only one completed LIVE entry.

Observed latest queue outcomes included:

- 16 distinct signals rejected with `overnight_watch_arm_failed:watch_returned_false`;
- approximately 33 distinct signals rejected with `mc_blocked:pending_entry_exists (...)`;
- 22 distinct signals left `ARMED`;
- one completed LIVE fill: `NOW`.

The system therefore did not rank all available opportunities and select the best few. Queue ownership, watcher-arm return semantics, duplicate pending-entry state, and trigger completion decided which candidate escaped the funnel.

## Required invariants

1. A recoverable watcher-arm failure must not terminally reject an otherwise valid opportunity.
2. A watcher `False` return must have one stable, explicit meaning. Ambiguous Boolean failure is prohibited.
3. A legitimate active pending entry must continue to prevent duplicate broker submissions.
4. A terminal, missing, expired, orphaned, or stale pending entry must not suppress a new valid opportunity indefinitely.
5. Ownership decisions must be scoped by exact `client_id`, `execution_mode`, symbol, side/direction, and canonical signal/order identity.
6. CALL and PUT opportunities must not conflict merely because they share a symbol unless the established direction/breach policy explicitly says they conflict.
7. No liveness repair may weaken broker idempotency.

## Scope

Implementation scope is limited to the existing overnight handoff / watcher
ownership seam and its durable state authority:

1. `ap_overnight_reeval.py`
2. `ap/order_monitor.py` pending-entry ownership classification
3. `ap/order_state_machine.py` stale-cleanup CAS and shared ownership predicates
4. the P0 workflow plus focused overnight/watcher ownership regressions

Do not touch:

- scanner score thresholds;
- intelligence or regime policy;
- selector quality thresholds;
- option premium caps;
- position sizing;
- exit engine;
- broker fill logic;
- proof-trades logic;
- unrelated queue state transitions.

## Structured watcher-arm result

Replace ambiguous Boolean interpretation at the caller boundary with an existing structured result if one already exists. Otherwise introduce the smallest local classification necessary.

Required outcomes:

### `ARMED`

- watcher owns the pending trigger;
- queue/order state remains active;
- idempotent repeat returns success.

### `ALREADY_WATCHING`

- same client/mode/order/signal is already owned;
- treated as idempotent success;
- no duplicate watcher or broker order.

### `RETRYABLE_NOT_ARMED`

Examples:

- transient in-memory registration race;
- temporary watcher capacity condition;
- recoverable dependency unavailability;
- a concurrent owner transition not yet observable.

Required behavior:

- do not terminally reject the signal;
- preserve or return it to a retryable queue state;
- stamp `next_retry_at` and exact diagnostics;
- bounded retry, no tight loop.

### `TERMINAL_CONFLICT`

Examples:

- proved opposite-side conflict under the established policy;
- invalid trigger geometry;
- identity mismatch that cannot safely be repaired.

Required behavior:

- terminal rejection is allowed;
- exact conflict identity and policy reason must be logged.

### `INTERNAL_ERROR`

- unexpected exception or persistence failure;
- do not claim strategy rejection;
- preserve incident diagnostics and avoid blind queue overwrite.

## Pending-entry ownership policy

`pending_entry_exists` may block only when the existing row/order is genuinely active for the same ownership key.

An existing entry is active only when all required conditions hold:

- identity matches the intended client and execution mode;
- status is a recognized non-terminal entry status;
- it is not expired by the existing session/TTL rules;
- watcher or durable recovery ownership is proved;
- no broker/OSM terminal state supersedes it.

The new candidate must be allowed to continue when the prior entry is:

- `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED`, `ERROR`, or otherwise terminal;
- missing from canonical OSM/broker state;
- an orphan with no watcher/recovery owner after the existing grace period;
- stale beyond the existing pending-trigger TTL;
- scoped to another client or execution mode;
- the opposite direction when policy permits both sides to remain eligible before directional breach.

## Persistence rules

- Never write queue success before watcher ownership is durable.
- Never blindly overwrite a terminal queue row.
- Retry state must remain distinguishable from strategy rejection.
- Preserve `client_id`, `execution_mode`, `signal_id`, `canonical_signal_id`, `local_order_id`, symbol, side, and timestamps.
- Compensation failures must be visible at CRITICAL severity.

## Required regression cases

1. Watcher returns explicit `ARMED`: queue remains active and succeeds once.
2. Same order already watched: idempotent success; no duplicate registration.
3. Watcher returns legacy `False` with no terminal evidence: retryable, not `REJECTED`.
4. Watcher raises a transient exception: internal/retry state, not strategy rejection.
5. Watcher proves opposite-side conflict: terminal conflict with exact competing identity.
6. Existing pending entry is terminal: new opportunity is not blocked.
7. Existing pending entry is stale and unowned: cleanup occurs once, new opportunity proceeds.
8. Existing pending entry is actively watcher-owned: new duplicate remains blocked.
9. Existing pending entry belongs to PAPER while candidate is LIVE: no cross-mode block.
10. Existing pending entry belongs to another client: no cross-client block.
11. Two same-symbol, same-direction duplicate signals resolve to one owner.
12. CALL and PUT before either directional breach follow the established dual-eligibility policy.
13. Cleanup persistence failure: fail closed without deleting the only durable owner.
14. Restart recovery reattaches ownership without manufacturing a second pending order.

## Production evidence required

Before this PR can become mergeable, the final PR description must contain:

- previous and new exact head SHA;
- exact changed-file list;
- focused test command and result;
- adjacent overnight/watcher/order-monitor regression result;
- replay/accounting for the 16 `watch_returned_false` signals;
- replay/accounting for the `pending_entry_exists` suppressions;
- proof that no duplicate LIVE broker submission is possible;
- explicit confirmation that selector, scoring, regime, exit logic, positions, and proof-trades are untouched;
- exact-head CI status.

## Current status

The production implementation and production-shaped regressions are present on
PR #404. The PR remains Draft, unmerged, and undeployed until the latest
amendment passes exact-head CI and receives a fresh whole-PR review. This change
repairs the overnight queue/watcher ownership seam; it does not by itself prove
scanner-to-broker trade flow, deployment state, or production profitability.
