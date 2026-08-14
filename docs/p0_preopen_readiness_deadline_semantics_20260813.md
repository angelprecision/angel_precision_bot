# P0 SPEC: Pre-open readiness deadline semantics

## Problem

Pre-open readiness currently classifies historical or otherwise non-authoritative `trade_queue.status = WATCHING` rows with no ENTRY order as a client-wide readiness error. The WATCHING query has no lower age bound, session/trading-date scope, execution-mode predicate, or lifecycle relevance check. Because WATCHING can legitimately exist before an ENTRY order is created, this diagnostic can freeze an otherwise healthy LIVE client.

This bug predates PRs #429, #434, and #445 and existed in the Aug 12 known-flowing baseline. Do not revert those PRs to address this issue.

## Required change

Replace the blanket `watching_orphans` warning with a read-only relevance boundary:

- A timezone-aware WATCHING row strictly older than the previous NYSE session,
  with no canonical ENTRY/order/position owner, is historical diagnostic debt.
  It remains visible as a warning and is never replayed.
- A current-session unresolved row is a BLOCK/HOLD.
- A prior-session or weekend/holiday-gap row is a BLOCK/HOLD unless exact
  canonical ownership is proven by the matching ENTRY order, broker/submission/
  fill evidence, or position.
- Recent missing mode evidence and any conflicting/invalid mode evidence are a
  BLOCK/HOLD. Explicit PAPER evidence never authorizes LIVE readiness.

The classifier uses ET session dates and the canonical NYSE holiday calendar;
missing, naive, malformed, or future timestamps are ambiguous and therefore
blocking.

Preserve the row/count detail in readiness output for observability.

## Fail-closed behavior that MUST remain unchanged

- `pending_trigger_without_watcher_ownership`
- missing LIVE morning handoff when authoritative
- missing overnight reevaluation when authoritative
- runner not alive / not initialized / queue worker missing
- entry watcher missing
- order state machine missing
- execution-mode mismatch
- pod/client mode mismatch
- selector quote identity unresolved
- broker credential safety behavior

## Non-goals / forbidden changes

- Do not reset or replay WATCHING rows.
- Do not mutate `trade_queue` lifecycle status.
- Do not alter `ap_recovery.py` LIVE no-replay policy.
- Do not change broker submit/cancel behavior.
- Do not change OSM behavior.
- Do not change positions or proof-trades persistence.
- Do not loosen `PENDING_TRIGGER` watcher-ownership checks.
- Do not mix the separate #434 intelligence `NoneType` fail-open defect into this P0.

## Expected implementation scope

Production file:
- `ap/preopen_readiness.py`

Tests:
- `tests/test_p0_preopen_autonomous_readiness.py` or the existing canonical pre-open readiness test module.

## Acceptance tests

1. 132 historical WATCHING rows plus one current-session ambiguous row -> LIVE `status == BLOCKED`; only the 132 historical rows are warning debt.
2. Friday-to-Monday and holiday-gap rows remain blocking until exact ownership is proven.
3. UTC timestamps are classified by their ET session date.
4. Recent missing mode and PAPER/LIVE conflict evidence remain HOLD/blocking.
5. Existing ENTRY, broker/submission/fill, or matching-position evidence is classified under canonical ownership, not orphan debt.
6. One LIVE `PENDING_TRIGGER` order without watcher ownership -> `status == BLOCKED`.
7. Zero WATCHING rows and otherwise healthy state -> `status == OK`.
8. Repeated startup/readiness execution is idempotent and performs no lifecycle mutation.
9. Test doubles assert zero broker ENTRY submit/cancel calls and zero mutation to orders, positions, proof_trades, or trade_queue lifecycle state.

## Release gate

HARD HOLD until exact-head CI passes and the final diff is audited for:
- no new broker call sites;
- no queue replay/reset;
- no client_id/execution_mode loss;
- no weakening of `PENDING_TRIGGER` watcher ownership;
- diagnostics still expose WATCHING row IDs/counts;
- `tests/test_p0_preopen_autonomous_readiness.py` runs in this exact-head P0 workflow.
