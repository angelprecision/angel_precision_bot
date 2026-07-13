# PR #324 Final Clarifications

These clarifications take precedence over both existing #324 documents wherever
wording conflicts:

- `docs/p0/watcher_invalidation_ownership_contract.md`
- `docs/p0/watcher_invalidation_ownership_amendment.md`

---

## 1. Branch starting point

`026c665b4b922eefe6781429e655c67d5416c88b` is the minimum contract ancestor, not
the implementation checkout target.

Implementation must begin from the current remote tip of:

    p0/watcher-invalidation-ownership-contract

Do not reset or check out the branch at `026c665...`.

---

## 2. FAILED must remain operationally owned

A `FAILED` completion may not leave an inert `INVALIDATED` object in `_pending`.

It must enter an explicit **cleanup-retry / ownership-quarantine state** that:

- remains physically registered
- is recognized by `has_order()` as owned
- holds the dedup key
- cannot fire `on_trigger` or submit
- persists the exact failure reason
- has `cleanup_retry_attempt`
- has `cleanup_retry_next_at`
- has `cleanup_retry_deadline`
- retries cleanup without hot-looping
- remains quarantined and owned if the deadline is exhausted

Deadline exhaustion may escalate diagnostics but may **not** release watcher
ownership.

---

## 3. No new external retry owner in #324

For this PR, `RETRY_OWNED` must retain the actual watcher in the registry.

Do not remove the watcher and transfer to a new durable worker unless an
already-existing production consumer is identified and tests prove:

- exact owner and lease
- restart pickup
- client and mode isolation
- bounded execution
- eventual ownership release
- dedup release

---

## 4. Complete existing reason assignments

Add these current production reasons to the canonical taxonomy:

### `overnight_open_recheck_data_timeout`

```
class  = INVALIDATED_TERMINAL
action = expire only after durable expiration acknowledgment
```

### `overnight_daily_validator_error`

```
class  = INVALIDATED_RETRYABLE
action = bounded retry
note   = terminal timeout must use a separate exact timeout reason
         (not overnight_daily_validator_error itself)
```

### `arm_below_stop`

```
class  = INVALIDATED_REARMABLE  — only after existing rearm eligibility succeeds
       = INVALIDATED_TERMINAL   — otherwise
note   = successful rearm should persist reason: arm_below_stop_reclaim_wait
```

### `watcher_invalidated`

```
class  = never accepted as the authoritative business reason
action = preserve the underlying raw reason alongside it
note   = absent underlying reason → FAILED ownership quarantine
         (not silent terminalization)
```

### `on_trigger_exhausted_3_attempts`

```
class  = INVALIDATED_TERMINAL   — but only after durable expiration is reread
         and verified
note   = expiration false/raise, or row still PENDING_TRIGGER after attempt
         → FAILED quarantine (not removal)
```

---

## 5. Additional hard-hold conditions

Do not mark PR #324 ready for review when any of these remain true:

- `FAILED` leaves an inert or non-owned watcher in `_pending`
- `RETRY_OWNED` transfers to a nonexistent or unproven external consumer
- the implementation branch was reset to `026c665...`
- any existing production invalidation reason remains unclassified
- the PR is marked ready before runtime code and blocking tests exist

---

## 6. Broker boundary

Existing local OSM pending-entry terminalization helpers may be used.

Do not add or modify any broker HTTP submit or broker HTTP cancel behavior.
