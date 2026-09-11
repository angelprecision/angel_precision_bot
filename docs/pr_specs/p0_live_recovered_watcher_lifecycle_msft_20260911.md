# P0: restore process-local lifecycle for recovered pre-breach watcher

## Status

DRAFT / HARD HOLD. Do not merge or deploy until fail-first replay, focused tests, exact-head P0, merge-ref P0, and rollback fail-first all pass on the unchanged final production SHA.

Base: `main@7a07f11a4e16a6d16ac0248cea4614559d5a054e`

## Production incident — Jason LIVE MSFT, 2026-09-11

Exact economic signal:

- client: `jasoncosby1@gmail.com`
- execution mode: `live`
- ticker/side: `MSFT CALL`
- signal_id: `c493c9dc-ceaa-4c86-8b62-08c3cc8bad5c`
- canonical_signal_id: `c493c9dc-ceaa-4c86-8b62-08c3cc8bad5c`
- local_order_id: `d6dd2c80-d0a1-45ca-bda9-4813ba7e64ba`
- trigger: `494.52`

The LIVE order was reconstructed by restart recovery before breach. `watcher_decision_audit` shows recovery rearm/classification at approximately 14:25 UTC and again 14:33 UTC.

At 15:15:31 UTC the recovered watcher legitimately confirmed the CALL breach. Production then emitted:

1. `CALL CONFIRMED`
2. lifecycle `ILLEGAL_TRANSITION ... NONE->TRIGGER_READY`
3. `WATCHER_TRIGGER_CALLBACK_ATTEMPT`
4. `WATCHER_TRIGGER_PERSISTENCE_RETRY`

The same cycle repeated on subsequent polls. No broker order was submitted. The durable LIVE row remained `PENDING_TRIGGER`, then restart recovery later canceled it as `pending_trigger_classifier:STUCK_TRIGGER_READY`.

The same canonical signal executed in PAPER. PAPER accounts selected `MSFT260911C00495000`, filled around 0.70, and recorded partial exits at 1.19. This is therefore LIVE execution parity loss, not scanner/signal disagreement.

## Root cause proven in current source

`ap_lifecycle.LEGAL_TRANSITIONS` does not permit `NONE -> TRIGGER_READY`. A restarted process must establish `NONE -> ADOPTED -> WATCHING` before a normal future breach may transition to `TRIGGER_READY`.

Current `ap_entry_watcher.watch(..., recovery_rearm=True)` stamps `signal_dict["__recovery_rearm"] = True` and calls ordinary watcher admission, but a successful recovered pre-breach admission does not restore the process-local lifecycle ledger.

The existing execution-confirmation test helper already documents this invariant explicitly: when a watcher is inserted without the normal lifecycle arm path, `_poll_active_signals()` will attempt `NONE -> TRIGGER_READY`; the helper manually seeds `NONE -> ADOPTED -> WATCHING` because otherwise the transition is illegal and submit is prevented.

## Binding invariant

A recovered pre-breach watcher is not executable ownership until both are true:

1. the exact watcher is successfully registered in `_pending` / dedup under the existing recovery identity and safety gates; and
2. its process-local lifecycle is restored to `WATCHING` through the existing legal recovery path.

For `recovery_rearm=True, materialization_resume=False` only:

- lifecycle `None` -> `ADOPTED` -> `WATCHING`;
- lifecycle `ADOPTED` -> `WATCHING`;
- lifecycle already `WATCHING` -> idempotent success;
- any other process-local lifecycle state -> fail closed and roll back only the watcher registration created by this call.

No new lifecycle state, durable authority, broker authority, generation, lease, or retry system may be introduced.

## Exact production change

### 1. `ap_entry_watcher.py`

Use the existing `ap_lifecycle` singleton and helpers. Add `signal_adopted` to the existing defensive lifecycle import alongside `signal_watching`.

Add one private helper on `APEntryWatcher`, for example:

`_restore_recovered_watcher_lifecycle(watched) -> bool`

The helper must:

- accept only a real `WatchedSignal` whose signal has `__recovery_rearm=True`;
- reject / no-op for `__materialization_resume=True` because #607 owns deferred retry adoption;
- require exact nonblank `signal_id` and ticker;
- inspect `_EW_LEDGER.current_state(signal_id)`;
- when state is `None`, call existing `signal_adopted(... owner=RECOVERY, reason="restart_recovery_loaded_existing_signal")`, then existing `signal_watching(... owner=WATCHER, reason="restored_to_watching_after_restart")`;
- when state is `ADOPTED`, call only `signal_watching(...)`;
- when state is already `WATCHING`, return success without another semantic transition;
- for `TRIGGER_READY`, `ENTRY_SUBMITTED`, terminal/error states, or any contradictory state, return false. Do not coerce them back to WATCHING.
- after each transition, reread `_EW_LEDGER.current_state(signal_id)` and require the expected state. A logged transition is not proof.

Wire this helper only after `add_signal()` has successfully admitted the exact recovered watcher and its dedup key. If lifecycle restoration fails, remove only the watcher instance created by this admission and release only its exact dedup key. Leave the durable `orders` row unchanged and return false.

Do not run this bridge for ordinary new arms. Do not run it for materialization-resume watchers.

### 2. `ap_entry_watcher/__init__.py`

No new lifecycle implementation here. The package shim is the production watcher and overrides `add_signal()`, but it delegates successful admission to `super().add_signal(...)`.

Only change this file if a narrowly necessary pass-through/result-provenance hook is required to identify the exact watcher instance created by the base admission. Do not duplicate lifecycle transitions in both base and shim.

Preferred result: base `ap_entry_watcher.py` owns the lifecycle restoration once, and the shim requires no production logic change.

### 3. `ap/pending_trigger_restart_recovery.py`

No production behavior change expected for this incident. It already calls:

`watcher.watch(plan, local_oid, recovery_rearm=True, registration_provenance_out=...)`

That call is the correct provenance boundary. Do not add a second lifecycle writer in restart recovery.

## Forbidden scope

Do not modify:

- `ap/order_state_machine.py`
- `ap_lifecycle.py`
- `ap_execution_core.py`
- `ap/pending_trigger_classifier.py`
- deferred materializer / selector retry policy
- broker adapters
- reconciler
- positions / proof trades
- database schema / migrations
- observability infrastructure

If implementation appears to require any of those files, STOP and report the missing primitive rather than broadening this PR.

## Required fail-first replay

Create one focused test file, preferably `tests/test_p0_live_recovered_watcher_lifecycle_msft.py`.

The first test must reconstruct the production sequence, not merely call the lifecycle helper directly:

1. Build exact broker-free `PENDING_TRIGGER` LIVE order identity matching the MSFT incident.
2. Reconstruct through the normal restart-recovery / `watch(... recovery_rearm=True)` path.
3. Prove watcher is in `_pending`.
4. On unpatched base, prove lifecycle is still `None` after recovery admission.
5. Feed two valid CALL breach polls.
6. Prove current base emits / produces illegal `NONE -> TRIGGER_READY` behavior and does not reach one successful trigger callback.

On patched head the exact same replay must prove:

- after recovery admission lifecycle is `WATCHING`;
- no `NONE -> TRIGGER_READY` event occurs;
- two confirming polls preserve normal `trigger_crossed_at` semantics;
- trigger authority persistence is attempted normally;
- callback is reachable exactly once when the authority write succeeds;
- no recovery-time broker submit/cancel occurs.

## Mandatory negative controls

1. ordinary non-recovery `watch()` behavior unchanged;
2. `materialization_resume=True` does not enter this lifecycle bridge;
3. missing signal_id -> fail closed, exact newly-created watcher registration rolled back;
4. missing ticker -> fail closed / no fabricated lifecycle;
5. process lifecycle already `WATCHING` -> idempotent success;
6. lifecycle `ADOPTED` -> `WATCHING` only;
7. lifecycle `TRIGGER_READY` / `ENTRY_SUBMITTED` / `ERROR` -> fail closed, no rewind to WATCHING;
8. failed watcher admission must not create ADOPTED/WATCHING lifecycle state;
9. lifecycle restoration failure after admission rolls back only this call's watcher + dedup ownership;
10. exact same signal cannot create duplicate recovered watcher ownership;
11. no broker submit/cancel/replace during lifecycle restoration;
12. no orders/positions/proof mutation by the lifecycle helper.

## Relationship to #580

Old PR #580 is archaeology and must not be rebased or salvaged.

This incident is the minimal pre-breach recovery-lifecycle slice exposed by the September 11 MSFT miss. It should land before any broader post-trigger-ready recovery recut. After this patch, the remaining clean #580 work can focus only on rows whose durable trigger authority already existed at restart, instead of also carrying basic `NONE -> ADOPTED -> WATCHING` reconstruction.

## Stop condition

Expected production scope is **one production file** (`ap_entry_watcher.py`), with `ap_entry_watcher/__init__.py` allowed only if exact package-shim admission provenance requires it.

If the patch exceeds two production files or introduces a new state machine, generation, lease, transaction framework, scheduler, recovery owner, or durable table field: STOP. The task has been broadened beyond the incident.
