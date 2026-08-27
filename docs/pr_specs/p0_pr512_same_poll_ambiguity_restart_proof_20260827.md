# P0 TEST GAP — PR #512 same-poll direction ambiguity must remain non-authoritative across restart

**Date:** 2026-08-27
**Base main SHA:** `25055d63e37e03683a447b42374324b0032ec38e`
**Status:** SPEC FIRST / TEST-ONLY BY DEFAULT / HARD HOLD UNTIL PROOF / DO NOT DEPLOY FROM THIS PR

## Why this PR exists
PR #512 is already merged on `main`. Exact-head P0 passed and no concrete production defect has been proven. One authority seam remains insufficiently proven: if CALL and PUT both confirm in the same poll with no independent ordering authority, #512 correctly produces `direction_claim_ambiguous_hold`; a process restart must not turn same-poll locally generated timestamps into durable winner authority.

## Binding rule
**Do not revert #512. Do not redesign #512. Do not change production code merely to satisfy this spec.** Start with a production-shaped behavioral proof against current main. If current code already satisfies the invariant, this PR must remain TEST-ONLY. If the test proves a real bug, STOP and report it before any runtime change. Do not auto-fix and merge in one step.

## Exact invariant
For one exact `(client_id, execution_mode, ticker)` ownership key, healthy ordinary CALL and PUT watchers may coexist pre-breach. If both confirm in the SAME poll and there was no durable preexisting `trigger_crossed_at` ordering before that poll:

```text
same poll CALL + PUT confirm
-> direction_claim_ambiguous_hold
-> zero winner callback
-> zero loser cancellation
-> zero broker submit
-> both durable orders remain PENDING_TRIGGER
```

After a process death/restart reconstructed from DURABLE state only:

```text
no direction becomes a durable confirmed winner solely from same-poll local timestamps
no side blocks the opposite as direction_claim_active from unproven ordering
no side cancels the other
no side reaches broker handoff from stale/unproven ordering
```

If both remain ambiguous after restart, HOLD again. If only one side later gains independently valid confirmed-breach authority, normal #512 winner logic may proceed.

## Required production-shaped regression
Add the focused test to:

```text
tests/test_p0_watcher_conflict_cancellation_proof.py
```

Preferred name:

```text
test_same_poll_ambiguous_direction_does_not_become_winner_after_restart
```

Use the real package watcher implementation. Do not duplicate the direction-claim algorithm in a test helper.

### Phase 1: establish same-poll ambiguity
Create production-shaped CALL and PUT durable rows with exact `client_id`, `execution_mode`, `signal_id`, `canonical_signal_id`, `local_order_id`, `ticker`, `direction`, `status=PENDING_TRIGGER`, no broker order id, and no submitted timestamp. Arm both ordinary watchers and drive quotes so both classify triggered in the same `_poll_active_signals()` batch with no preexisting crossed-at evidence.

Assert:

```text
callbacks == 0
broker submits == 0
cancel_pending_entry calls == 0
both watcher states PENDING
both durable rows PENDING_TRIGGER
reason_code includes direction_claim_ambiguous_hold
process-local claim status == ambiguous_hold
```

Inspect the durable `orders.meta` and explicitly assert intended truth for `trigger_crossed_at`, `trigger_crossed_at_provenance`, `trigger_confirmed_at`, `first_breach_bid`, and `first_breach_ask`. Do not assume these fields are persisted. Prove the actual contract.

### Phase 2: restart boundary
Destroy the first watcher instance. Create a NEW watcher using only durable row/signal state available after restart. Do not reuse old watcher objects, `_direction_claims`, `_direction_poll_context`, in-memory timestamps, or retry timers. Rehydrate through the closest existing production recovery path; if none is available in the harness, reconstruct new `WatchedSignal` objects only from durable row truth and document why that matches production.

Immediately assert:

```text
no process-local won claim
neither side is treated as durable winner solely from same-poll ambiguity
zero cancellation
zero callback
zero broker submit
```

Poll again with a quote shape that makes both sides ambiguous. Expected: `direction_claim_ambiguous_hold` again, zero cancellation, zero callback, zero broker submit.

### Phase 3: positive control
Preserve the existing legitimate restart behavior: a truly durable confirmed winner with valid identity-bound provenance must still block an opposite after restart. Do not solve the ambiguity case by deleting restart ownership.

## Required negative controls
1. Reverse registration order and get the same post-restart result.
2. LIVE/PAPER isolation remains exact.
3. Different clients on the same ticker do not share direction ownership.
4. Missing/invalid client or mode fails closed when an opposite exists.
5. Durable confirmed winner remains authoritative.
6. Durable loser terminalization remains respected.
7. Same-side duplicate/rearm behavior remains unchanged.
8. Open protection does not invent a winner.
9. No direct broker submit/cancel authority is added.

## Mandatory money-path assertions
For the ambiguous restart case:

```text
OSM ENTRY submit calls = 0
raw broker POST calls = 0
broker order ids created = 0
positions mutations = 0
proof_trades mutations = 0
trade_queue mutations = 0
```

## Scope
Expected changed production files: **NONE**.
Expected implementation diff if current code passes:

```text
tests/test_p0_watcher_conflict_cancellation_proof.py
```

That file is already registered in P0 CI, so no workflow edit should be needed.

Explicitly out of scope unless the fail-first regression proves a real bug: `ap_entry_watcher.py`, `ap_entry_watcher/__init__.py`, `ap/order_state_machine.py`, `ap_execution_core.py`, master control, selector, risk, sizing, broker adapter, positions, proof_trades, queue, exits, reconciler, scanners, intelligence, and all trading thresholds.

## Interaction with #524
#524 owns deferred materialization sequencing. Do not absorb #524 work here and do not modify deferred materialization behavior.

## Validation
Run:

```bash
python -m pytest -q tests/test_p0_watcher_conflict_cancellation_proof.py
```

Then exact-head P0 CI.

If the regression fails, STOP before production modification and report: exact failing assertion; durable row state before/after restart; which field became winner authority; callback/cancel/broker handoff counts; smallest runtime seam needing correction.

## Completion report
Post:

```text
BASE SHA
HEAD SHA
exact changed files
production files changed YES/NO (expected NO)
current-main result
same-poll ambiguity result before restart
durable orders.meta fields after ambiguity
post-restart result
registration-order parity
LIVE/PAPER isolation
cross-client isolation
durable confirmed-winner positive control
broker submit count
broker cancel count
orders terminalization count
positions/proof/queue mutation counts
focused pytest
exact-head P0 CI
final recommendation: KEEP #512 / HOLD #512 FOR FIX
```

## Merge rule
Mergeable only if it remains test-only, exact-head CI is green, and the production-shaped restart invariant is proven. If a production defect is discovered, HARD HOLD and report before runtime changes.

**Do not merge, revert, deploy, or rewrite #512 automatically from this task.**
