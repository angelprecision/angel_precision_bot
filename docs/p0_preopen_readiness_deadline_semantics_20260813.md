# P0 SPEC: Pre-open readiness deadline semantics

## Problem

Pre-open readiness currently classifies historical or otherwise non-authoritative `trade_queue.status = WATCHING` rows with no ENTRY order as a client-wide readiness error. The WATCHING query has no lower age bound, session/trading-date scope, execution-mode predicate, or lifecycle relevance check. Because WATCHING can legitimately exist before an ENTRY order is created, this diagnostic can freeze an otherwise healthy LIVE client.

This bug predates PRs #429, #434, and #445 and existed in the Aug 12 known-flowing baseline. Do not revert those PRs to address this issue.

## Required change

Treat `watching_orphans` as diagnostic/warning information only. Do not add `watching_rows_missing_orders_recommend_new_rescue` to `errors` solely because these rows exist.

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

1. 133 historical WATCHING rows without ENTRY orders, with every real readiness dependency healthy -> `status == OK`; WATCHING rows remain present in details/warnings.
2. One LIVE `PENDING_TRIGGER` order without watcher ownership -> `status == BLOCKED`.
3. 133 historical WATCHING rows plus one unowned LIVE `PENDING_TRIGGER` -> BLOCKED due to the unowned pending trigger only.
4. Zero WATCHING rows and otherwise healthy state -> `status == OK`.
5. Repeated startup/readiness execution is idempotent and performs no lifecycle mutation.
6. PAPER behavior remains separately classified and is not promoted into LIVE authority.
7. Test doubles assert zero broker ENTRY submit/cancel calls.
8. Test doubles assert zero mutation to orders, positions, proof_trades, or trade_queue lifecycle state from this classification change.

## Release gate

HARD HOLD until exact-head CI passes and the final diff is audited for:
- no new broker call sites;
- no queue replay/reset;
- no client_id/execution_mode loss;
- no weakening of `PENDING_TRIGGER` watcher ownership;
- diagnostics still expose WATCHING row IDs/counts.
