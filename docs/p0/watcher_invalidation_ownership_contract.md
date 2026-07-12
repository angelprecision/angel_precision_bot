# P0 — Watcher Invalidation Must End in Durable Terminal, Retry, or Rearm Ownership

## Goal

After this PR, every watcher leaving normal active ownership must end in exactly one
truthful lifecycle outcome:

1. **Durably terminalized** — order moved out of `PENDING_TRIGGER`, exact reason
   persisted, watcher removed, dedup released.
2. **Durably retry-owned** — watcher remains registered or a durable retry owner
   exists; retry reason, attempt, and deadline are persisted; dedup remains held
   where required.
3. **Durably rearm-owned** — watcher remains registered in explicit rearm mode;
   rearm reason, attempt count, and expiry are persisted; dedup remains held.

A watcher must never be removed, deactivated, or have its dedup key released merely
because an invalidation decision was made.  Ownership may only be released after the
next durable owner or terminal outcome is proven.

---

## Confirmed production-shape failures

### Failure A — LIVE overnight quote outage creates an inert watcher

In `ap_entry_watcher.py`, generic/non-daily overnight revalidation can:

1. Receive no valid LIVE bid, ask, or last.
2. Persist `overnight_live_quote_unavailable`.
3. Set `w.state = WatchState.INVALIDATED`.
4. Release the dedup key.
5. `continue` without adding the watcher to terminal cleanup.
6. Leave the watcher object in `_pending`, but with `is_active == False`.

The watcher is now physically present but operationally ownerless:

- it will not poll,
- it will not trigger,
- it will not retry,
- `on_invalidate` is not called,
- the order can remain `PENDING_TRIGGER`,
- the dedup key is released.

### Failure B — detached deferred watcher is "restored" to PENDING

In normal polling:

1. `WatchedSignal.check()` returns `INVALIDATED`.
2. The watcher is removed from `APEntryWatcher._pending`.
3. `APExecutionCore._on_signal_invalidate()` runs afterward.
4. For some deferred-contract invalidations, execution core sets:
   - `watched.state = WatchState.PENDING`
   - `watched.breach_count = 0`
   - returns without cancelling the order.

The object is no longer registered in `_pending`, so changing its state to `PENDING`
does not restore watcher ownership.

The durable row may remain:

```
status=PENDING_TRIGGER
watcher owner=none
retry owner=none
terminal outcome=none
```

### Failure C — callback or order cleanup failure loses ownership

Invalidated/expired watchers can be removed before:

- `on_invalidate` succeeds,
- `on_expire` succeeds,
- `cancel_pending_entry()` succeeds,
- `expire_pending_entry()` succeeds.

If cleanup raises or returns false, the watcher has already been removed and the row
can remain ownerless.

### Failure D — trigger and stop on the same poll is mislabeled

`WatchedSignal.check()` currently allows:

1. Trigger confirmation to set `TRIGGERED`.
2. Stop logic on the same poll to overwrite state with `INVALIDATED`.
3. Breach evidence to be suppressed.
4. `on_trigger` never to run.
5. The reason to appear as an ordinary "stop broke before trigger."

This must become an explicit trigger/stop collision outcome.  It must not submit
automatically, but it must preserve the fact that the trigger was crossed or confirmed.

---

## Scope

Primary files to audit and modify:

```
ap_entry_watcher.py
ap_execution_core.py
ap/pending_trigger_classifier.py
ap/order_state_machine.py
tests/test_p0_watcher_invalidation_ownership.py
```

Potentially modify an existing lifecycle or reason-code module if one already provides
the correct shared home.  Do not create a second competing lifecycle framework.

## Non-goals

This PR must not:

- call the broker submit endpoint,
- create a new order,
- mutate an order into submitted state,
- create a position,
- write a proof trade,
- alter normal contract selection,
- modify quantity,
- alter execution price,
- alter client account routing,
- broaden into a watcher rewrite, strategy change, scoring change, or architecture
  refactor.

---

## Required invalidation taxonomy

Implement or centralize these lifecycle classifications (not necessarily order statuses):

### INVALIDATED_TERMINAL

Use only when authoritative market/setup evidence proves the opportunity is no longer
valid.

```
stop_bid_below_call_stop
stop_ask_above_put_stop
overnight_daily_invalidated
overnight_too_far_from_trigger
arm_drift
invalid_side
prior_high_breached
prior_low_breached
both_sides_breached
rearm_window_exhausted
```

### INVALIDATED_RETRYABLE

Use for transient unavailability or recoverable infrastructure failure.

```
overnight_live_quote_unavailable
overnight_open_data_unavailable_retry_later
snapshot_unavailable
missing_prior_levels
quote_fetch_failed
validator_provider_timeout
temporary_database_failure
```

### INVALIDATED_REARMABLE

Use only when an explicit reclaim policy is active.

```
temporary_wrong_side_of_stop
arm_below_stop_reclaim_wait
```

### INVALIDATED_ALREADY_BREACHED

Use where the setup was otherwise valid but the trigger already occurred or a
trigger/stop collision was observed.

```
arm_already_through_trigger
overnight_daily_already_through_trigger
overnight_premarket_breached
trigger_stop_same_poll_collision
```

### INVALIDATED_NO_WATCHER_OWNER

This is an invariant violation and must generate an error/critical diagnostic.

```
PENDING_TRIGGER with no active watcher
cleanup failed after watcher removal
detached watcher state restored to PENDING
INVALIDATED object left inert in _pending
```

---

## Required implementation

### 1. Introduce an acknowledged watcher completion result

The watcher callbacks must return an explicit result instead of being treated as
fire-and-forget.

Use an enum, constants, or a small typed result with at least:

```
TERMINALIZED   — order durably left PENDING_TRIGGER (CANCELED/EXPIRED/REJECTED/ERROR_TERMINAL)
RETRY_OWNED    — bounded retry durably scheduled, or watcher remains with explicit retry state
REARMED        — watcher remains registered in explicit non-triggering reclaim state
FAILED         — no terminal transition, retry ownership, or rearm ownership was proven
```

`FAILED` must keep the watcher registered and dedup held.

Do not interpret a plain Python `return` with no value as success.

### 2. Do not remove watchers before callback acknowledgment

Required ordering:

```
check()
→ classify decision
→ invoke callback / durable lifecycle handler
→ inspect acknowledged result
→ remove + release dedup  only for TERMINALIZED
→ retain or explicitly transfer  for RETRY_OWNED / REARMED
→ retain + emit CRITICAL diagnostic  for FAILED
```

Apply this invariant to:

- normal polling invalidation,
- normal polling expiration,
- EOD expiration,
- overnight revalidation,
- arm-time cleanup shim,
- trigger callback exhaustion cleanup.

Do not change normal successful trigger removal behavior except where necessary to
preserve the same ownership rule.

### 3. Fix generic LIVE overnight quote-unavailable behavior

`overnight_live_quote_unavailable` must not create an inert `INVALIDATED` object.

Required MVP behavior:

- Treat the first LIVE quote outage as retryable.
- Keep the watcher registered.
- Keep dedup ownership.
- Persist: reason code, first-failure timestamp, latest-failure timestamp, retry
  attempt, next-retry timestamp, retry deadline.
- Retry until a bounded deadline.

At success: run normal revalidation → arm / rearm / already-breached / structural
invalidation.

At deadline exhaustion: terminalize with exact reason
`overnight_live_quote_unavailable_timeout`, cancel/expire the row durably, then
remove watcher and release dedup.

### 4. Remove detached deferred PENDING restoration

Delete or replace any logic that merely does:

```python
watched.state = WatchState.PENDING
return
```

after the watcher was removed from the registry.

For every deferred-contract invalidation:

- structural invalidation → terminalize,
- transient input failure → retry-owned,
- reclaim-eligible condition → rearm-owned,
- already crossed trigger → already-breached terminal outcome,
- unknown LIVE reason → fail closed,
- unknown PAPER reason → retain ownership only through explicit retry/rearm outcome.

If returning a watcher to `PENDING` is genuinely required, prove all of the following
before doing so:

```
watcher is physically present in APEntryWatcher._pending
watcher state is PENDING or explicit rearm state
dedup key is held
order remains PENDING_TRIGGER
retry/rearm metadata is durable
client_id is unchanged
execution_mode is unchanged
```

Prefer making the lifecycle decision before registry removal rather than removing and
re-inserting.

### 5. Preserve exact invalidation reason downstream

Do not collapse every terminal invalidation to `watcher_invalidated` or
`signal_invalidated_in_poll_loop`.

Those may remain diagnostic event names, but the exact business reason must survive
in:

```
orders.last_error (or canonical terminal reason field)
orders.meta.watcher_audit.reason_code
orders.meta.watcher_invalidation_class
signal status metadata
lifecycle diagnostics
queue/opportunity metadata where applicable
```

Preserve both:

```json
{
  "watcher_invalidation_class": "INVALIDATED_TERMINAL",
  "watcher_invalidation_reason": "PRIOR_HIGH_BREACHED",
  "watcher_invalidation_source": "overnight_daily_validator"
}
```

Do not alter `client_id`, `client_email`, `execution_mode`, `paper`, `signal_id`,
`plan_id`, or `local_order_id`.

### 6. Add explicit trigger/stop collision handling

When the same coherent quote poll proves both trigger and stop conditions:

- do not allow stop logic to silently overwrite confirmed trigger state,
- do not submit the trade automatically,
- persist classification `INVALIDATED_ALREADY_BREACHED`, reason
  `trigger_stop_same_poll_collision`,
- preserve all breach evidence:
  `trigger_crossed_at`, `trigger_confirmed_at`, `trigger_price`, `first_breach_bid`,
  `first_breach_ask`, `current_bid`, `current_ask`, `stop_level`, `quote_age_ms`,
  `ticker`, `side`.
- terminalize the watcher only after the durable order transition succeeds.

### 7. Centralize reason classification

Create one authoritative classifier or constant set consumed by:

```
ap_entry_watcher.py
ap_execution_core.py
ap/pending_trigger_classifier.py
```

The restart classifier and live callback classifier must agree on whether a reason is
terminal / retryable / rearmable / already-breached / invariant-violation.

Unknown LIVE reasons fail closed but preserve the exact unknown reason string.

---

## Safety constraints

### Identity and mode preservation

Every order mutation must remain scoped to the existing order and preserve:

```
local_order_id  client_id  client_email
execution_mode  paper/live mode
signal_id  plan_id
```

No cross-client cleanup.  No PAPER row may affect LIVE ownership.  No LIVE row may
affect PAPER ownership.

### Diagnostics preservation

Do not delete existing watcher audit data.  Add normalized fields without removing
raw reasons or quote context.

---

## Required tests

Create production-shape tests using actual callback and watcher registry behavior.

### Test 1 — LIVE overnight quote outage remains owned

Given LIVE watcher, generic/non-daily overnight setup, `orders.status=PENDING_TRIGGER`,
zero/missing quote.  Prove watcher remains in `_pending`, remains retry-owned, dedup
held, order remains `PENDING_TRIGGER`, retry metadata persisted, `on_invalidate` not
falsely treated as terminal success.

### Test 2 — Quote recovers before deadline

Prove first poll quote unavailable → second poll valid → watcher revalidation resumes
→ watcher can become valid and active → no duplicate watcher admitted.

### Test 3 — Quote retry deadline expires

Prove timeout reason is exact, order leaves `PENDING_TRIGGER`, watcher removed only
after durable transition, dedup released only after durable transition.

### Test 4 — Cancel helper returns false

Prove watcher remains registered, dedup held, order remains owned, critical/failed
acknowledgment produced.

### Test 5 — Cancel helper raises

Same invariant as Test 4.

### Test 6 — Expire helper returns false or raises

Same invariant.

### Test 7 — Invalidate callback raises

Prove watcher is not removed and does not become ownerless.

### Test 8 — Deferred benign/retryable invalidation

Prove watcher remains physically registered rather than only having its detached
object state set to `PENDING`.

### Test 9 — Deferred structural invalidation

Prove exact reason terminalizes the row even when contract is `DEFERRED:<SYMBOL>`.

### Test 10 — Trigger and stop on same poll

Prove no broker submit, `classification=INVALIDATED_ALREADY_BREACHED`,
`reason=trigger_stop_same_poll_collision`, breach evidence preserved, order
terminalized only after callback acknowledgment.

### Test 11 — Exact reason preservation

Prove an underlying reason such as `stop_bid_below_call_stop` survives in downstream
order metadata and terminal diagnostics instead of becoming only `watcher_invalidated`.

### Test 12 — Mode and client isolation

Two rows, different `client_id` and `execution_mode`, same ticker/signal shape.
Prove cleanup affects only the intended row.

### Test 13 — No ownerless PENDING_TRIGGER outcome

Truth-table test covering all callback results (`TERMINALIZED`, `RETRY_OWNED`,
`REARMED`, `FAILED`).  Assert no result produces:
`status=PENDING_TRIGGER` AND no registered watcher AND no retry owner AND no rearm
owner.

---

## Verification

```bash
python3 -m pytest -q tests/test_p0_watcher_invalidation_ownership.py
python3 -m pytest -q tests/test_p0_pending_trigger_lifecycle_integrity.py
python3 -m pytest -q tests/test_p0_overnight_watcher_cleanup.py
python3 -m pytest -q tests/test_entry_watcher_audit.py
```

Report: exact head SHA, changed files, tests run, tests passed, whether any broker
submit code changed, whether behavior is active or flag-off.

---

## Acceptance criteria

This PR is complete only when all are true:

1. No invalidated or expired watcher is removed before durable cleanup acknowledgment.
2. No dedup key is released before durable terminalization or explicit ownership transfer.
3. LIVE overnight quote unavailability is bounded retry, not inert invalidation.
4. Deferred invalidation cannot restore a detached object and pretend ownership exists.
5. Trigger/stop collision preserves breach evidence.
6. Exact reason survives downstream.
7. No invalidation path submits an order.
8. No path mutates the wrong client or execution mode.
9. No `PENDING_TRIGGER` row can be produced without watcher, retry, or rearm ownership.
10. Tests exercise actual registry membership and actual callback return behavior, not
    only pure helper functions.
