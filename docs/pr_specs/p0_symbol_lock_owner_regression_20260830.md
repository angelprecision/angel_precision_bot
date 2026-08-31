# P0 Symbol-Lock Owner Regression Repair

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## Proven regression

Merged PR #473 previously made same-symbol entry locks owner-scoped by persisting the exact `local_order_id` as `owner_id` in `ap/state.py` and requiring an exact owner match before filled-entry cleanup could delete the lock.

Current main has regressed to timestamp-only lock payloads and unscoped `release_symbol_lock(client_id, symbol)`. An old/recovered fill can therefore delete a newer same-symbol entry lock after TTL reacquisition.

## One repair only

Expected production scope: `ap/state.py` plus the smallest exact callers needed to pass/verify `owner_id`.

1. Acquire symbol lock with exact current entry `local_order_id` owner token.
2. Persist owner token durably in the lock payload.
3. Release/delete only when stored owner token exactly matches the releasing order's `local_order_id`.
4. Missing, malformed, legacy, or mismatched owner token preserves the lock and emits diagnostics.
5. Do not use timestamps as ownership authority.
6. No selector, score, sizing, risk threshold, broker submit/cancel, queue, proof, exit policy, or intelligence changes.

## Required proof

- A acquires symbol lock; TTL expires; B reacquires; late/recovered A cleanup cannot delete B lock.
- Exact owner release deletes its own lock.
- Legacy timestamp-only payload is preserved rather than guessed.
- Concurrent same-symbol acquisition remains serialized.
- LIVE/PAPER/client identity unchanged.
- Real PostgreSQL race test where practical plus exact-head P0 CI.

Do not restore the whole #473 implementation. Later identity/recovery PRs already replaced other pieces.