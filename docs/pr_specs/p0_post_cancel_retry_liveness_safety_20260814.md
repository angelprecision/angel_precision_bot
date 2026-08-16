# P0 SPEC — Post-cancel retry must use one canonical ENTRY authority

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

Original branch base was older than current main. Before implementation, rebase this branch onto exact current `main@f26d31cef3d3bf5d3c5d5ff16fb260f245602f89` and re-read all touched paths. Do not implement against the stale base.

## Audit amendment — binding

The 2026-08-15 trade-lifecycle audit proved that the current retry submit path is not merely "unproven". It is unsafe.

Current production path:

```text
ap/order_monitor.py::_check_armed_retries()
  -> _submit_armed_retry()
  -> ap.execution.process_signal()
  -> ap.db.insert_order(status='NEW')
  -> ap.execution._submit_order_with_retry()
  -> broker.place_order(buy_to_open)
  -> ap.db.update_order(status='ACK')
```

Three defects are one architectural failure:

1. `NEW` / `ACK` are a second order-state vocabulary. Canonical OSM active queries use `CREATED`, `PENDING_TRIGGER`, `SUBMITTED`, `ACKNOWLEDGED`, `PARTIAL_FILL`.
2. `ap.execution._submit_order_with_retry()` performs blind retry POSTs after exceptions and passes no idempotency tag. An ambiguous broker acceptance can create duplicate LIVE entries.
3. current-main `ap.db.insert_order()` does not persist top-level `execution_mode`.

Therefore the old spec clause allowing Claude to "prove `process_signal()` safe and retain it" is deleted.

**`ap.execution.process_signal()` is forbidden as the post-cancel retry submit authority.**

Do not patch `NEW`, patch `ACK`, add a tag, and persist mode while leaving the second engine alive. F4/F5/F7 are one defect: duplicate entry authority. Collapse the retry back into the canonical control path.

PR #450 (`fix: persist execution mode on legacy order inserts`) is now superseded for the retry problem. Do not merge #450 as the solution to this P0. Persisting mode on a duplicate authority does not remove the duplicate authority.

## One job

A broker-confirmed no-fill ENTRY cancel may receive **at most one** bounded retry only if the setup remains provably valid, and that retry must re-enter Angel Precision's canonical ENTRY lifecycle before any broker POST.

No retry broker POST may originate from `ap/execution.py`.

## Required canonical lifecycle

The retry must converge back into the same architecture ordinary current-main entries use:

```text
retry intent
-> one durable owner claim
-> current client/mode/risk/duplicate eligibility
-> fresh market/contract truth
-> canonical OSM order row/status vocabulary
-> watcher/trigger semantics where required by the ordinary route
-> OSM hardened broker submit boundary
-> fill monitor
-> position ownership
```

The final broker POST must be owned by `APOrderStateMachine`'s hardened submit path with:

- broker tag derived from canonical local order identity;
- ambiguous-outcome lookup by tag;
- no blind replacement POST;
- canonical status transition;
- durable `execution_mode`.

If Claude discovers another current-main canonical wrapper around that exact OSM boundary, reuse it. Do not invent another submit helper.

## Routing requirement

The preferred implementation is to hand the claimed retry back to the **existing canonical queue/execution-control path** rather than directly constructing a broker order in the monitor.

Before choosing the exact seam, trace current main:

- `ap/queue.py::_dispatch()`
- Master Control evaluation/revalidation
- contract selector
- `APOrderStateMachine.create_entry_order()`
- entry watcher registration / trigger callback
- `APOrderStateMachine.submit_existing_entry()` / hardened submit helper

Then use the smallest existing entry point that executes those same controls for a retry payload.

If re-enqueueing through `trade_queue` is the canonical safe seam, requirements are:

- use one deterministic retry idempotency key, e.g. identity equivalent to `(client_id, canonical_signal_id, retry_root, retry_attempt=1)`;
- preserve `canonical_signal_id` while giving the retry generation its own exact execution/order identity;
- queue insertion must be idempotent under duplicate monitor ticks/restart;
- claim success does not mean broker submit success;
- queue worker remains the sole downstream owner.

If current canonical dedupe semantics make a same-opportunity retry impossible through queue, **STOP and report the exact blocker**. Do not fall back to `process_signal()`.

A direct call from order monitor to `APOrderStateMachine.submit_*` is not acceptable unless the same Master Control, contract freshness, risk/capital, trigger/revalidation, and identity gates are demonstrably executed first through an existing canonical wrapper. The monitor is an orchestrator, not a broker submitter.

## Existing post-cancel policy defects retained from original spec

### A. MISSED_MOVE is terminal

Remove `missed_move` from retryable reasons and classify it as non-retryable.

```text
STALE_ENTRY_CANCEL MISSED_MOVE
-> CANCELED
-> retry decision ABORT
-> zero queue retry generation
-> zero broker retry POST
```

### B. One canonical retry attempt

Canonical field: `retry_attempt` singular.

Read legacy `retry_attempts` only for compatibility. Never let a new local order reset lineage.

Maximum: one retry generation per original retry root.

### C. Alignment must be proven

Required before ARM:

- exact CALL/PUT direction;
- positive original trigger/entry reference;
- positive fresh underlying observation;
- current setup has not invalidated target/stop/thesis;
- any required market evidence is current under existing policy.

`None`/missing evidence is `ALIGNMENT_UNPROVEN`, not allow.

### D. Cancel must be broker-confirmed terminal

A cancel request is not enough. The original broker order must be proven terminal and non-filled before retry intent is armed.

Late fill or partial fill wins over retry. Any positive broker fill quantity routes to fill/position ownership, not a new entry generation.

### E. ARMED -> SUBMITTING/QUEUED claim is CAS authority

Discovery SELECT is not authority.

Before any retry handoff, atomically claim the canceled original order's retry intent:

```text
client exact
kind=ENTRY
status=CANCELED
execution_mode exact live|paper
meta.retry_status=ARMED
retry_ready_at due
retry_attempt=1
```

Persist JSONB patch, not full metadata replacement.

The state name may be `SUBMITTING` or `QUEUED` depending on the chosen canonical handoff, but it must mean exactly one durable owner has taken responsibility.

Claim loser and DB failure perform zero queue/broker work.

### F. Preserve identity lineage

Durable retry lineage must preserve:

- client_id
- execution_mode
- signal_id
- canonical_signal_id
- original local order id / retry root
- retry attempt=1
- prior broker order id
- cancellation reason and terminal proof
- side/direction
- ticker
- trigger / breach generation where present
- plan/materialization identity where present

No PAPER -> LIVE or LIVE -> PAPER adoption.

## Required removal / quarantine of legacy authority

After implementation, the current post-cancel retry path must contain **zero calls** to:

```python
from ap.execution import process_signal
process_signal(...)
```

Add a static regression asserting this for `ap/order_monitor.py`.

Do not delete `ap/execution.py` in this PR unless source search proves it has no remaining production callers and deletion is independently safe. The objective is to remove it from this LIVE-reachable retry money path.

Also search all production callers of `ap.execution.process_signal()` and report them in the PR description. Any other LIVE-reachable caller discovered is a separate blocker and must be declared rather than silently ignored.

`ap/manual_trades.py` is known dead/broken and is not an excuse to retain the retry route.

## Expected production scope

Authorized starting set:

- `ap/post_cancel_retry.py`
- `ap/order_monitor.py`
- the **single canonical handoff owner actually required** by current architecture, likely one of:
  - `ap/queue.py`, or
  - `ap_execution_core.py`, or
  - `client_runner.py` solely for dependency wiring.

`ap/order_state_machine.py` should be reused, not reimplemented. Edit it only if a missing narrowly-defined retry-generation API is proven necessary.

`ap/execution.py` is not to be hardened into another authority in this PR.

If more than the minimal handoff/wiring set is required, report the expansion before coding further.

## Required tests

Create/update `tests/test_p0_post_cancel_retry_liveness_safety.py`.

Minimum cases:

1. MISSED_MOVE -> no retry generation.
2. unknown cancel reason -> no retry.
3. unproven alignment -> no retry.
4. lost alignment -> no retry.
5. broker cancel request but terminal state unproven -> no retry.
6. late original fill -> fill path, no retry.
7. original partial fill -> fill/position path, no full duplicate retry.
8. first valid no-fill cancel -> exactly one retry intent.
9. singular retry_attempt=1 -> second retry blocked.
10. legacy plural retry_attempts=1 -> second retry blocked.
11. two concurrent ARMED claims -> one owner.
12. crash after claim before canonical handoff -> restart cannot create two retry generations.
13. duplicate monitor ticks -> one queue/canonical handoff maximum.
14. retry identity mismatch client -> zero handoff.
15. retry mode mismatch -> zero handoff.
16. malformed mode -> zero handoff.
17. canonical handoff creates only OSM vocabulary statuses; no `NEW` or `ACK` order row.
18. retry order has top-level execution_mode exact.
19. final broker submit carries canonical tag/local identity.
20. ambiguous broker POST outcome -> lookup/reconcile; zero blind second POST.
21. successful retry -> exactly one broker buy-to-open maximum.
22. pre-broker risk failure -> zero broker POST and durable exact reason.
23. fresh quote/contract rejection -> zero broker POST.
24. no position/proof mutation before broker fill.
25. queue/handoff diagnostics preserve retry root and prior broker order.
26. static test: order_monitor contains no `process_signal` import/call.
27. static/source inventory: all remaining production `process_signal` callers are reported.
28. ordinary non-retry queue entry behavior remains unchanged.
29. PAPER retry remains PAPER through broker adapter selection.
30. LIVE retry remains LIVE and cannot use sandbox identity.

Run adjacent:

- phase5 post-cancel retry
- phase9 retry wire-in
- queue hardening/current P0 queue tests
- OSM broker submit boundary tests
- execution-mode identity tests
- fill conversion tests
- duplicate submit/idempotency tests

## Money-path safety checklist

- Live behavior: **YES.** Retry route changes.
- Flag: existing `ENTRY_RETRY_ENABLED` remains emergency off switch; do not rely on it as correctness.
- Broker submit/cancel: cancel remains existing; retry broker submit must be OSM-only.
- Orders: canonical OSM rows only; no NEW/ACK writes from retry.
- Positions/proof: no pre-fill mutation.
- Queue: may be used only as canonical handoff, with deterministic idempotency and exact lineage.
- client_id/execution_mode: exact durable columns, not metadata guesses.
- Diagnostics: preserve cancellation, claim, retry root, canonical handoff, OSM local id, broker id, ambiguity disposition.
- PAPER/LIVE taxonomy: strict.
- Could this make Jason trade junk? Not if implemented correctly: no missed-move retry, no unproven alignment, full canonical gates rerun, and no blind broker retry.

## Supersession / overlap

- **#450**: do not merge as the fix for F7. This PR eliminates the retry caller that depended on legacy mode-less inserts.
- **#440**: owns broker cancel/replace race semantics and must land/rebase coherently. This PR must not add its own cancel owner.
- **#473/#480**: own fill->position handoff; late/partial fill observed during retry cancellation must route there.

## Claude implementation instruction

1. Rebase this spec branch onto exact current main.
2. Read PR #440, #473, #480 and current queue/OSM execution path before editing.
3. Reproduce `NEW`, `ACK`, mode omission, and ambiguous blind retry from current main.
4. Remove `process_signal()` from retry authority.
5. Route the single claimed retry through the existing canonical entry-control seam.
6. Preserve one retry maximum and anti-chase rules.
7. Return exact broker-call counts and all mutation paths.

Before requesting review, update the PR with:

- exact current-main base/head SHA;
- changed files;
- caller inventory for `process_signal`;
- canonical handoff chosen and why;
- one-owner concurrency proof;
- client/mode lineage proof;
- broker ambiguity proof;
- focused/adjacent test counts;
- exact-head CI;
- fresh MERGE / HOLD / HARD HOLD verdict.

No merge, deploy, migration, environment mutation, or production-data mutation is authorized.