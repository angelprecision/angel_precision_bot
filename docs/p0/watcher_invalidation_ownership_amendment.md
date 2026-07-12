# PR #324 Final Implementation Amendment

P0: Prevent watcher invalidation from leaving ownerless PENDING_TRIGGER rows

Work only on the existing PR #324 branch:

    branch: p0/watcher-invalidation-ownership-contract
    current contract head: 026c665b4b922eefe6781429e655c67d5416c88b
    base: main
    required base SHA: 7bdc7064d2e5ef1b2d26517f3137fbbb9adecdcc

Do not create a new PR.
Do not rebase PRs #321 or #322 as part of this work.
Preserve the existing contract document: docs/p0/watcher_invalidation_ownership_contract.md
Implement the contract in runtime code and tests.

---

## Objective

After this amendment, every invalidated or expired entry watcher must end in exactly
one proven ownership outcome:

1. Durable terminalization
2. Durable bounded retry ownership
3. Durable rearm ownership
4. Failed ownership transfer with the original watcher still registered

The following state must be impossible:

    orders.status = PENDING_TRIGGER
    AND no active watcher is registered
    AND no durable retry owner exists
    AND no durable rearm owner exists

Watcher removal and dedup release are consequences of a proven durable outcome.
They are not allowed merely because an invalidation decision was produced.

---

## Strict scope

Primary runtime files:

    ap_entry_watcher.py
    ap_execution_core.py
    ap/pending_trigger_classifier.py

Modify only when required:

    ap/order_state_machine.py

Required new test file:

    tests/test_p0_watcher_invalidation_ownership.py

Update the blocking P0 workflow only if the new test is not automatically included:

    .github/workflows/p0_regression.yml

Do not modify:

- broker submit logic
- broker cancel request logic
- contract selector policy
- contract pricing
- position sizing
- capital reservation
- queue admission
- position lifecycle
- proof_trades
- exit engine
- stale active-position quote behavior
- intelligence or scoring

This amendment must produce zero broker entry POSTs.
Do not split this work into multiple PRs.

---

## Required architecture

### 1. Add an explicit watcher completion acknowledgment

Create one small authoritative result type, enum, or immutable object with these
outcomes:

    TERMINALIZED
    RETRY_OWNED
    REARMED
    FAILED

Suggested shape:

```python
@dataclass(frozen=True)
class WatcherCompletionResult:
    outcome: str
    reason_code: str
    local_order_id: str | None = None
    retry_next_at: str | None = None
    retry_deadline: str | None = None
    detail: str | None = None
```

A plain `None`, implicit return, exception, truthy object, or arbitrary dictionary
must not be interpreted as successful cleanup.

Normalize callback results at one boundary.

Legacy callbacks returning `None` must become `FAILED` unless the watcher can
independently prove a durable terminal, retry, or rearm state.

---

### 2. Verify callback acknowledgment against actual state

Do not trust a callback's declared result by itself.

Before removing a watcher or releasing its dedup key, verify the result against both:

- actual watcher registry state
- actual durable order state

#### TERMINALIZED verification

A TERMINALIZED acknowledgment is accepted only when a reread proves:

- `local_order_id` matches
- `client_id` matches
- `execution_mode` matches
- `status` is no longer `PENDING_TRIGGER`
- no active recovery or submit owner requires the watcher to remain
- exact terminal reason is persisted

Permitted durable terminal states (existing authoritative statuses only):

    CANCELED  EXPIRED  REJECTED  ERROR

Do not invent a new order status.

If the callback says TERMINALIZED but the durable row remains `PENDING_TRIGGER`:

- convert result to `FAILED`
- retain watcher in `_pending`
- retain dedup key
- emit CRITICAL ownership diagnostic

#### RETRY_OWNED verification

A RETRY_OWNED result is accepted only when at least one of the following is proven:

**A.** The watcher is physically present in `APEntryWatcher._pending`,
is in an explicit retry state, and its dedup key remains held.

**OR**

**B.** Durable metadata proves a bounded retry owner, including:
`retry_reason`, `retry_attempt`, `next_retry_at`, `retry_deadline`,
`owner_identity`, `client_id`, `execution_mode`.

If retry metadata persistence returns false or raises:

- `outcome = FAILED`
- watcher remains registered
- dedup remains held

#### REARMED verification

A REARMED result is accepted only when:

- watcher remains physically registered
- watcher is `PENDING` or in an explicit rearm state
- dedup key remains held
- order remains `PENDING_TRIGGER`
- rearm reason, attempt, and expiry/deadline are persisted
- `client_id` and `execution_mode` are unchanged

Changing a detached Python object to `PENDING` is not rearm ownership.

#### FAILED behavior

`FAILED` always means:

- retain watcher
- retain dedup
- do not cancel solely because the callback failed
- do not expire solely because the callback failed
- persist an ownership-failure diagnostic where safely possible
- emit CRITICAL log

---

### 3. Change watcher removal ordering

For every invalidation and expiration path, use this order:

```
watcher.check()
→ classify reason
→ invoke lifecycle callback
→ normalize callback acknowledgment
→ verify acknowledgment against registry and durable row
→ then decide removal
```

Removal policy:

| Result | Watcher | Dedup |
|---|---|---|
| TERMINALIZED | remove | release |
| RETRY_OWNED | retain (unless transferred to durable worker) | retain |
| REARMED | retain | retain |
| FAILED | retain | retain + emit CRITICAL |

Apply to:

- normal invalidation
- normal expiration
- EOD expiration
- overnight revalidation
- arm-time cleanup
- deferred watcher invalidation
- trigger callback exhaustion cleanup
- callback exceptions
- OSM cancel/expire false returns
- OSM cancel/expire exceptions

Do not alter normal successful trigger submission behavior except where needed to
preserve ownership acknowledgment.

---

### 4. Extend the existing pending-trigger classifier

Do not create a second competing invalidation classifier.

Extend `ap/pending_trigger_classifier.py`.

This module must remain the shared authority used by:

- `ap_entry_watcher.py`
- `ap_execution_core.py`
- restart recovery
- watcher health/orphan inspection

Replace generic invalidation handling with one canonical reason-to-class mapping.

Required classifications:

    INVALIDATED_TERMINAL
    INVALIDATED_RETRYABLE
    INVALIDATED_REARMABLE
    INVALIDATED_ALREADY_BREACHED
    INVALIDATED_NO_WATCHER_OWNER

Preserve existing pending-trigger row classifications where other callers depend on
them. Add an adapter or mapping rather than breaking existing imports.

The live watcher and restart classifier must agree on each reason.

---

### 5. Canonical reason taxonomy

#### INVALIDATED_TERMINAL

Use only when authoritative evidence proves the setup is invalid:

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
    overnight_live_quote_unavailable_timeout

#### INVALIDATED_RETRYABLE

Use for transient data or infrastructure failures:

    overnight_live_quote_unavailable
    overnight_open_data_unavailable_retry_later
    snapshot_unavailable
    missing_prior_levels
    quote_fetch_failed
    validator_provider_timeout
    temporary_database_failure

#### INVALIDATED_REARMABLE

Use only when the existing explicit rearm policy permits reclaim:

    temporary_wrong_side_of_stop
    arm_below_stop_reclaim_wait

Do not silently enable rearm when the existing rearm feature/policy is disabled.

#### INVALIDATED_ALREADY_BREACHED

Use when trigger evidence exists but automatic entry must not occur:

    arm_already_through_trigger
    overnight_daily_already_through_trigger
    overnight_premarket_breached
    trigger_stop_same_poll_collision

#### INVALIDATED_NO_WATCHER_OWNER

Use for invariant violations:

    PENDING_TRIGGER without watcher/retry/rearm owner
    cleanup failure after premature removal
    detached object restored to PENDING
    inert INVALIDATED object left in _pending
    callback claims terminalization but row remains PENDING_TRIGGER
    retry/rearm acknowledgment without durable or registry ownership

---

### 6. Unknown LIVE reason policy

Unknown LIVE invalidation reasons must fail closed without silently deleting a
potentially valid setup.

Required behavior:

- classification = `INVALIDATED_NO_WATCHER_OWNER` or explicit `UNKNOWN_LIVE_INVALIDATION`
- completion outcome = `FAILED`
- watcher retained
- dedup retained
- order remains `PENDING_TRIGGER`
- exact original unknown reason preserved
- CRITICAL diagnostic emitted

Do not automatically terminalize a LIVE order only because the classifier does not
recognize the reason.

Unknown PAPER reasons must also retain ownership through `FAILED`, explicit retry,
or explicit rearm. Do not silently remove them.

---

### 7. Fix overnight LIVE quote unavailability

Replace the inert invalidation behavior for `overnight_live_quote_unavailable` with
bounded retry ownership.

Required metadata:

    watcher_invalidation_class   = INVALIDATED_RETRYABLE
    watcher_invalidation_reason  = overnight_live_quote_unavailable
    watcher_invalidation_source
    watcher_retry_owner
    watcher_retry_attempt
    watcher_retry_first_failed_at
    watcher_retry_last_failed_at
    watcher_retry_next_at
    watcher_retry_deadline

Environment-controlled bounds with conservative defaults:

    WATCHER_OVERNIGHT_QUOTE_RETRY_DELAY_SECONDS=30
    WATCHER_OVERNIGHT_QUOTE_RETRY_DEADLINE_SECONDS=180
    WATCHER_OVERNIGHT_QUOTE_RETRY_MAX_ATTEMPTS=6

Clamp malformed or unsafe values.

While retry remains valid:

- watcher remains in `_pending` and operationally active or explicit retry-wait
- dedup remains held
- order remains `PENDING_TRIGGER`
- no `on_invalidate` terminal acknowledgment is emitted

When a valid quote returns:

- clear retry state truthfully
- run normal overnight revalidation
- continue to valid watch / rearm / already-breached / terminal invalidation

At deadline or attempt exhaustion:

- reason = `overnight_live_quote_unavailable_timeout`
- invoke durable terminalization
- verify durable row left `PENDING_TRIGGER`
- then remove watcher and release dedup
- if terminal persistence fails → `FAILED`, retain watcher and dedup

---

### 8. Remove detached deferred PENDING restoration

Find every path equivalent to:

```python
watched.state = WatchState.PENDING
watched.breach_count = 0
return
```

...after the watcher was already removed from `_pending`. Delete or replace those paths.

Lifecycle decision must occur before registry removal.

A deferred watcher may return to `PENDING` only after proving all of the following:

- the exact watcher object remains in `_pending`
- dedup key remains held
- order is still `PENDING_TRIGGER`
- retry or rearm metadata is durable
- `local_order_id`, `client_id`, `execution_mode` match

| Deferred invalidation type | Required outcome |
|---|---|
| Structural | terminalize with exact reason |
| Transient | `RETRY_OWNED` |
| Reclaim-eligible | `REARMED` |
| Unknown / persistence-failed | `FAILED` |

---

### 9. Trigger and stop on the same poll

Change `WatchedSignal.check()` so stop logic cannot silently overwrite a confirmed
trigger state.

When the same coherent quote proves both trigger and stop conditions:

- do not call broker submit
- do not return ordinary stop invalidation
- `classification = INVALIDATED_ALREADY_BREACHED`
- `reason = trigger_stop_same_poll_collision`

Preserve:

    trigger_crossed_at          trigger_confirmed_at
    trigger_price               first_breach_bid
    first_breach_ask            current_bid
    current_ask                 current/observed underlying price
    stop_level                  quote_age_ms
    ticker                      side
    local_order_id              signal_id
    client_id                   execution_mode

The callback must durably terminalize the order with the exact collision reason.
The watcher is removed only after durable terminalization is reread and verified.

If terminalization fails: `FAILED`, watcher retained, dedup retained, zero broker
submits.

---

### 10. Exact reason preservation

Persist the exact business reason in all applicable destinations:

    orders.last_error
    orders.meta.watcher_invalidation_class
    orders.meta.watcher_invalidation_reason
    orders.meta.watcher_invalidation_source
    orders.meta.watcher_audit.reason_code
    existing signal decision/status metadata
    existing lifecycle diagnostics

Do not replace underlying reason with only:

    watcher_invalidated
    signal_invalidated_in_poll_loop
    cleanup_failed

Those may remain event labels; the original reason must survive alongside them.

Do not mutate `client_id`, `client_email`, `execution_mode`, `paper/live mode`,
`signal_id`, `plan_id`, or `local_order_id`.

Every SQL mutation must remain scoped to the exact existing order and client. Where
execution mode is available, include it in the mutation predicate or verify it before
mutation.

---

## Required tests

Create: `tests/test_p0_watcher_invalidation_ownership.py`

Tests must exercise real watcher registry membership and real callback return handling.
Do not satisfy these requirements only with pure classifier tests.

### Test 1 — LIVE overnight quote outage remains owned

Given LIVE watcher, generic/non-daily overnight setup, `PENDING_TRIGGER` order, zero
quote.  Prove: watcher in `_pending`, retry-owned, dedup held, order `PENDING_TRIGGER`,
bounded retry metadata exists, no terminal callback falsely accepted, zero broker
submits.

### Test 2 — Quote recovers before deadline

First poll quote unavailable → second poll valid → normal revalidation resumes →
watcher becomes valid/active → retry state closed truthfully → duplicate admission
blocked.

### Test 3 — Quote retry deadline expires

Prove: exact timeout reason, durable order terminalization succeeds, watcher removed
afterward, dedup released afterward.

### Test 4 — Cancel helper returns false

Result `FAILED`.  Watcher registered, dedup held, order remains owned.

### Test 5 — Cancel helper raises

Same ownership proof as Test 4.

### Test 6 — Expire helper returns false

Same ownership proof.

### Test 7 — Expire helper raises

Same ownership proof.

### Test 8 — Invalidate callback raises

Prove watcher and dedup remain.

### Test 9 — Callback lies about terminalization

Callback returns `TERMINALIZED` but order reread remains `PENDING_TRIGGER`.  Expected:
normalized result `FAILED`, watcher registered, dedup held, critical diagnostic.

### Test 10 — Retry metadata persistence fails

Classification is retryable but metadata write returns false or raises.  Expected:
`FAILED`, watcher registered, dedup held.

### Test 11 — Detached deferred restoration is impossible

Prove a retryable deferred invalidation leaves the actual watcher object in `_pending`.
Changing a removed object to `PENDING` is not accepted.

### Test 12 — Deferred structural invalidation

Prove exact terminal reason survives even when contract is `DEFERRED:<SYMBOL>`.

### Test 13 — Trigger and stop collision

Prove: `classification=INVALIDATED_ALREADY_BREACHED`,
`reason=trigger_stop_same_poll_collision`, breach evidence preserved, zero broker
submit calls, durable terminalization verified before removal.

### Test 14 — Exact reason preservation

Use reason `stop_bid_below_call_stop`.  Prove it survives in order metadata and
terminal diagnostics.

### Test 15 — Unknown LIVE reason

Expected: `FAILED`, watcher retained, dedup retained, exact unknown reason persisted,
zero terminalization based only on unknown classification.

### Test 16 — Client and execution-mode isolation

Two orders, different `client_id` and `execution_mode`, same ticker/signal shape.
Prove only the intended row is changed.

### Test 17 — Completion-result truth table

Cover all four results.  For every result, assert this condition is never true:

    status=PENDING_TRIGGER
    AND watcher not registered
    AND no retry owner
    AND no rearm owner

### Test 18 — Dedup lifecycle

Prove: retry/rearm/failed → second watch admission rejected; verified terminalization
→ dedup released; new lifecycle may then be admitted only under existing policy.

---

## Regression requirements

```bash
python3 -m pytest -q tests/test_p0_watcher_invalidation_ownership.py
python3 -m pytest -q tests/test_p0_pending_trigger_lifecycle_integrity.py
python3 -m pytest -q tests/test_p0_overnight_watcher_cleanup.py
python3 -m pytest -q tests/test_entry_watcher_audit.py
python3 -m pytest -q tests/test_p0_deferred_breach_lifecycle_completion.py
python3 -m pytest -q tests/test_p0_seam4_e2e_deferred_lifecycle.py
```

Then the complete blocking P0 workflow command from `.github/workflows/p0_regression.yml`.

Also:

```bash
python3 -m py_compile \
  ap_entry_watcher.py \
  ap_execution_core.py \
  ap/pending_trigger_classifier.py \
  ap/order_state_machine.py
git diff --check
```

Add the new ownership test file to the blocking P0 GitHub Actions workflow if not
already included.

---

## Hard-hold conditions

Do not mark PR #324 ready for review if any of these remain true:

- watcher removed before durable acknowledgment verification
- dedup released while retry/rearm/failed ownership remains
- callback `TERMINALIZED` accepted without rereading the order
- callback `None` accepted as successful cleanup
- `overnight_live_quote_unavailable` leaves an inert `INVALIDATED` object
- retry metadata failure removes the watcher
- detached object is changed to `PENDING` after registry removal
- unknown LIVE reason automatically deletes the setup
- trigger/stop collision can call `on_trigger` or broker submit
- exact invalidation reason is collapsed
- classifier logic is duplicated across modules
- `client_id` or `execution_mode` isolation is unproven
- new test is absent from blocking CI

---

## Required final report

After implementation, report exactly:

- final commit SHA
- base SHA
- head SHA
- files changed
- production behavior changed
- callback result type introduced
- all callback callsites updated
- classifier changes
- overnight retry defaults and environment variables
- whether `ap/order_state_machine.py` changed and why
- whether any broker submit/cancel code changed
- whether any queue/position/proof_trade code changed
- focused test commands and counts
- complete P0 CI result
- `py_compile` result
- `git diff --check` result

Also provide a path-by-path explanation for:

- normal invalidation
- normal expiration
- overnight quote outage
- deferred retryable invalidation
- deferred structural invalidation
- callback failure
- cleanup persistence failure
- trigger/stop collision
- unknown LIVE reason

Do not claim the PR is merge-ready merely because tests pass. Confirm the actual
runtime invariant:

**No `PENDING_TRIGGER` row can lose watcher ownership without durable terminal,
retry, or rearm ownership.**
