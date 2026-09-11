# P0: converge durable terminal entry row to exact watcher

## Scope and base

This recut is based directly on committed GitHub `main`:

`cef7cb8bade47d491c19811b917ebdc9b4b7872a`

The retired PR #597 branch is incident archaeology only. The replacement
remains Draft / HARD HOLD pending independent audit.

Production scope is limited to `ap_entry_watcher.py`. The focused proof is
`tests/test_p0_pr597_terminal_watcher_recut.py`. This spec is the only other
new artifact, plus one line adding the focused test to the existing P0
inventory. The current-main #606 trigger-authority CAS and #607 due-retry
authority are unchanged.

## Failure contract

A deferred watcher can remain behavior-active after recovery terminalizes its
exact `orders` row. The existing trigger-authority CAS then correctly refuses
to operate on a non-`PENDING_TRIGGER` row, but the watcher remains in
`_pending` and retains its dedup key indefinitely.

The TMO reproduction is local order
`b9e29854-1c97-43d6-986b-eef0506bdf5a`, with `status=CANCELED`,
`last_error=restart_stuck_trigger_ready_no_broker_proof`, no broker order ID,
and no submitted timestamp. On unchanged `main`, the watcher remains pending
after the row is encountered. On the recut, exact terminal truth removes only
that watcher before dispatch.

## Shared terminal truth

`APEntryWatcher` performs one read-only exact-row probe before trigger dispatch
and reuses the same probe in callback-result verification. Terminal statuses
are exactly `REJECTED`, `EXPIRED`, `CANCELED`, and `ERROR`.

Canonical reason sources are `orders.last_error` and these root `meta` fields:

- `restart_recovery_terminal_reason`
- `terminal_reason`
- `reason_code`
- `final_reason`
- `watcher_invalidation_reason`

`meta.materialization_reason` remains legacy evidence only when no canonical
reason exists. A different historical materialization reason does not conflict
with a consistent canonical reason. `meta.watcher_audit.reason_code` is never
terminal proof; in particular, `trigger_ready` alone is insufficient.

Multiple canonical reasons, a terminal status without a recognized reason, an
unreadable row/metadata surface, or incomplete/contradictory identity is a
fail-closed HOLD.

## Exact removal fence

Before removal, the probe must match the exact watcher on non-empty:

- `local_order_id`
- client identity
- `LIVE` / `PAPER` execution mode
- `signal_id`
- `canonical_signal_id` when durable canonical identity is present

The row must be an ENTRY row when `kind` is supplied. A ticker, contract, or
side match is never sufficient. Removal is identity-based, releases only that
watcher's dedup key, and is idempotent.

## Broker-handoff fence

Terminal-looking rows with broker ownership evidence remain reconciliation
holds. Evidence includes a broker order ID, submitted timestamp, `submit_intent`
metadata, broker submit key or payload hash, and broker-ready authority on the
normal or nested materialization metadata surface. The watcher does not submit,
cancel, mutate positions, or mutate proof trades on this path.

## Ordering and race behavior

After `WatchedSignal.check()` builds the completed batch and before the package
direction hook, trigger audit, open protection, trigger-authority CAS, or
`on_trigger`, deferred candidates are probed:

- exact proven terminal row: remove only that watcher and its dedup key;
- ordinary exact broker-free `PENDING_TRIGGER`: pass through unchanged to the
  existing #606 CAS path;
- every other result: retain the watcher, reset it to `PENDING`, and suppress
  callback work for that poll.

The existing package direction hook then sees the terminal watcher absent and
can prune its own stale process-local claim. The package shim is not changed.

The same probe runs after a callback claim is returned. Thus a
`PENDING_TRIGGER → #606 CAS success → callback begins → concurrent terminal
recovery → stale SUBMITTED return` race resolves to `TERMINAL_DURABLE`, and the
exact watcher is removed. A later terminal proof always outranks the stale
callback word.

## Required proof inventory

The focused suite covers the TMO fail-first/repaired path, all canonical reason
aliases, legacy materialization evidence, missing reason, `trigger_ready`-only
evidence, conflicting reasons, each required identity mismatch, broker-handoff
evidence, unrelated same-ticker survival, ordinary `PENDING_TRIGGER` positive
control, mid-callback terminalization, and repeated-observation idempotency.

The merge gate is the focused suite plus the unchanged canonical P0 inventory
on the final production SHA, exact-head and merge-ref runs, the existing
rollback fail-first job, and `git diff --check`. The branch remains Draft / HARD
HOLD; green checks do not authorize merge, deployment, or runtime promotion.
