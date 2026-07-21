# P0: Restore fresh quote monitoring and automatic recovery for LIVE open positions

## Status and sequencing

This is a draft implementation contract. Do not merge it as documentation-only work.

Implementation must begin only after:

1. PR #385 is merged and its exact-head P0 workflow is green;
2. the Jason and Jose trade-flow fixes ahead of this PR are merged or deliberately rebased around;
3. this branch is rebased onto the resulting current `main`;
4. the implementer records the new base SHA in the PR description before changing production code.

Do not implement this PR against the stale base currently recorded on the draft. Do not copy old pre-#385 quote-truth logic back into the repository while resolving conflicts.

Keep the PR in draft until all required tests are executed. Do not merge without explicit approval from Angel.

## Production incident

On July 21, 2026, Jason's LIVE account opened a position, but the operator severity view reported the position quote as stale. The bot was not managing the position from continuously fresh market truth, and the operator manually exited it.

This is a live-money protective-monitoring failure. A stale warning is not sufficient if the system does not recover the quote stream, prove why recovery cannot proceed, or escalate that the position is not under fresh monitoring.

## Relationship to PR #385

PR #385 defines what exit decisioning may do when option or underlying truth is stale, missing, non-executable, or provenance-unproven.

PR #387 addresses the upstream liveness failure:

> Why did an open LIVE position remain stale instead of being adopted, polled, refreshed, recovered, or escalated?

Do not duplicate or weaken #385. Reuse its canonical provider-timestamp normalization, quote validity, snapshot bridge, hard-exit reference, and stale-exit protections after rebase.

A separate entry-side incident is required only if durable evidence shows that the quote used for the final LIVE broker BUY submission was stale or that the final LIVE submit gate was bypassed. Do not mix that investigation into this PR.

## Non-goals

- No exit-threshold changes.
- No take-profit, stop-loss, trail, hard-stop, grace-window, or EOD-policy changes.
- No entry-selection, signal-admission, risk, contract-selection, sizing, or watcher redesign.
- No new general readiness framework.
- No second quote authority.
- No second exit worker.
- No automatic flatten solely because quotes are unavailable.
- No broad refactor of `APExitEngine`.
- No replacement of the existing per-client `ClientRunner` lifecycle.
- No use of receipt time as proof that provider market data is fresh.

## Existing production seams that must be traced

The implementation must inspect and preserve the existing ownership model rather than creating parallel infrastructure.

### `client_runner.py`

Inspect and, only where required, amend:

- `ClientRunner._start_position_quote_monitor()`
- `ClientRunner._quote_monitor_healthy()`
- `ClientRunner._set_entry_permission()`
- the existing runtime health loop that checks worker, fill monitor, core, and QPM liveness
- runner shutdown and cleanup paths

Current behavior that must be audited:

- `_start_position_quote_monitor()` returns when any existing monitor reports `is_alive()`, even if it is bound to the wrong client, mode, broker, or exit-engine instance.
- monitor creation and restart are not protected by an explicit generation lock.
- restarting QPM through the same function may also create a duplicate `ExitReliabilityMonitor` unless lifecycle ownership is made idempotent.

### `ap/position_quote_monitor.py`

Inspect and, only where required, amend:

- `APPositionQuoteMonitor.__init__()`
- `start()` / `stop()` / `is_alive()` / `is_healthy()`
- `last_cycle_age_sec()` / `metrics_snapshot()` / `health_snapshot()`
- `request_immediate_refresh()`
- `_loop()`
- `_refresh_once()`
- `_fetch_batch_cached()` / `_fetch_batch()`
- `_classify_health()`
- `_prune_closed()`

Known current weaknesses that must not remain:

1. `_loop()` updates `_last_cycle_ts` in `finally`, including failed cycles. Therefore `is_healthy()` can remain true while every broker cycle is failing.
2. the health registry key is currently the global static name `ap_quote_monitor`; one client's heartbeat can mask another client's stalled monitor.
3. `_classify_health()` classifies and alerts, but stale state does not itself guarantee a direct recovery fetch.
4. `request_immediate_refresh()` is symbol-scoped and only evicts shared cache entries; it does not maintain a position-scoped recovery generation or prove that recovered truth reached the exact position.
5. `_SHARED_CACHE` is process-global and keyed only by symbol. Audit whether LIVE and PAPER clients or distinct market-data transports can consume each other's cached quote payloads. If yes, namespace the cache by stable data-source identity plus symbol.
6. current health is mostly based on general service heartbeat and quote timestamps. The service may be alive while one open LIVE position is omitted.

### `ap_exit_engine.py`

Inspect the existing methods, but keep changes narrow:

- `active_positions()`
- position fill/adoption methods
- broker-open repair/adoption path
- `attach_quote_monitor()` or equivalent attachment seam
- `apply_quote_snapshots()`
- the exit-engine wake/kick seam
- existing stale protective-retry path and critical event emitter

The exit engine remains the owner of `ManagedPosition`. QPM must not create a second position object as decision authority.

### `ap_health_registry.py`

Change only if required to support a unique per-client monitor key. A valid key must distinguish at least:

```text
client_id | execution_mode | monitor_instance_id
```

Do not continue relying on one global `ap_quote_monitor` heartbeat for multiple clients.

### `app.py` and operator severity surface

Modify only the existing health/severity read model needed to expose the canonical monitoring record. Do not create a second dashboard API when an existing endpoint can be extended.

### Tests and workflow

Add one focused production-seam test module:

```text
tests/test_p0_live_position_quote_monitor_recovery.py
```

Add it to `.github/workflows/p0_regression.yml`.

Update existing tests only when behavior intentionally changes. Do not weaken assertions merely to make the suite green.

## Required investigation before coding

Before changing production logic, reconstruct the July 21 Jason incident as far as durable evidence permits.

Capture:

- client ID and execution mode;
- position ID and option symbol;
- broker entry order ID and fill timestamp;
- position-row creation and update timestamps;
- exit-engine adoption timestamp or first log proving presence in `active_positions()`;
- QPM instance identity and thread start timestamp;
- first QPM cycle after fill;
- whether the position appeared in that cycle's active-position set;
- option and underlying symbols included in the batch request;
- broker response, exception, 429, or empty result for each failed cycle;
- raw provider timestamps and receipt timestamps separately;
- first and last successful option quote;
- first and last successful underlying quote;
- when severity first became degraded, stale, or blind;
- every immediate-refresh request and whether it was accepted, coalesced, backoff-suppressed, or executed;
- whether shared cache returned the quote;
- whether the snapshot was applied to the same `ManagedPosition` used by exit evaluation;
- QPM heartbeat age and consecutive cycle failures;
- whether multiple QPM or exit-engine instances existed for Jason;
- whether the monitor was bound to the correct broker, exit engine, client, and execution mode.

Post an investigation note to the PR before implementing. It must identify the first proven broken seam or state that production evidence was unavailable and list the exact code paths being hardened defensively.

Do not begin with a guessed architecture change.

## Root-cause classification

The final PR description must classify the incident into one or more of these stable categories:

```text
ADOPTION_MISSING
POSITION_OMITTED_FROM_CYCLE
QPM_THREAD_STALLED
QPM_CYCLES_FAILING
WRONG_CLIENT_OR_MODE_BINDING
WRONG_EXIT_ENGINE_BINDING
BROKER_EMPTY_OR_ERROR
BROKER_RATE_LIMITED
SHARED_CACHE_STALE
SHARED_CACHE_CROSS_SOURCE
PROVIDER_TIMESTAMP_STALE
SNAPSHOT_NOT_APPLIED
EXIT_ENGINE_NOT_WOKEN
UNKNOWN_INSUFFICIENT_EVIDENCE
```

Do not claim a root cause that the evidence does not prove.

## Required identity contract

Every monitoring and recovery action must use durable position identity:

```text
client_id | execution_mode | position_id | option_symbol
```

Rules:

- `position_id` is mandatory for LIVE recovery ownership.
- contract symbol alone is not position identity.
- two positions using the same contract must retain separate recovery state.
- a reopened contract must start with a new recovery generation.
- a wrong-client or wrong-mode position must fail closed and emit a critical event; it must not be silently polled or mutated.
- snapshots must target the exact exit-engine position by position ID, with contract used as a verification field rather than the sole locator.

## Required per-monitor state

Each QPM instance must expose immutable instance identity created once in `__init__()`:

```text
monitor_instance_id
client_id
execution_mode
bound_broker_identity
bound_exit_engine_identity
thread_generation
started_at
last_cycle_attempt_ts
last_cycle_success_ts
last_cycle_completed_ts
consecutive_cycle_failures
last_cycle_error
```

`is_alive()` and `is_healthy()` must not collapse these concepts:

- thread alive;
- loop advancing;
- cycles completing;
- broker calls succeeding;
- every open LIVE position receiving fresh quote coverage.

A monitor may be thread-alive but coverage-degraded. Expose that honestly.

## Required per-position monitoring state

Maintain one position-scoped record, keyed by the full identity contract, containing at least:

```text
client_id
execution_mode
position_id
option_symbol
underlying
monitor_instance_id
first_seen_ts
last_seen_in_active_positions_ts
last_poll_attempt_ts
last_poll_success_ts
last_option_success_ts
last_underlying_success_ts
last_snapshot_applied_ts
last_exit_engine_wake_ts
option_provider_ts
underlying_provider_ts
option_receipt_ts
underlying_receipt_ts
consecutive_failed_cycles
consecutive_stale_cycles
last_failure_code
last_broker_error
recovery_state
recovery_generation
recovery_attempt_count
last_recovery_attempt_ts
next_recovery_at
last_recovery_result
critical_escalated_at
```

Use a dataclass or a clearly typed internal structure. Do not scatter these fields across unrelated dictionaries without a canonical snapshot method.

Prune state only after the position is conclusively absent from `active_positions()` and closed. Do not prune merely because a contract disappeared from one batch response.

## Required adoption behavior

### New fill

When `APExitEngine` adopts a newly filled LIVE position:

1. the position must appear in `active_positions()` immediately after adoption;
2. the attached QPM must be kicked without waiting for its normal interval;
3. the next QPM cycle must record `first_seen_ts` and `last_poll_attempt_ts` for that exact position;
4. the position must be included in option and underlying requests;
5. successful truth must be applied to the exact exit-engine object;
6. the exit engine must be woken after a fresh recovery/adoption snapshot, regardless of whether price movement exceeds ordinary wake thresholds.

Do not add a second fill listener if the existing adoption method can call one narrow `on_position_adopted()` or `kick()` hook.

### Broker repair or restart hydration

When broker truth discovers an open position missing from in-memory state:

1. adopt it through the canonical exit-engine repair path;
2. verify client, mode, position ID, quantity, and contract;
3. attach it to the existing per-client QPM;
4. kick QPM;
5. require a monitoring-adoption event on the next cycle;
6. keep quote validity false until fresh provider truth arrives.

Loading a position into the exit engine without proving QPM adoption is incomplete.

## Required quote-fetch behavior

### Ordinary polling

Ordinary cycles may use the shared cache, provided the cache is correctly namespaced and its TTL is receipt-cache TTL only. Cache freshness must never become provider-market freshness.

### Direct recovery fetch

Add one explicit direct-recovery fetch path, preferably a narrow method such as:

```python
_fetch_batch_direct(symbols, *, recovery_generation, position_identity)
```

or an equivalent typed interface.

It must:

- bypass the shared cached payload;
- preserve rate-limit backoff authority;
- perform no uncontrolled retry loop;
- record whether the attempt was executed, coalesced, or backoff-deferred;
- preserve raw provider timestamps and receipt timestamps separately;
- return structured success/failure evidence rather than only `{}`;
- never mark a stale provider value fresh because the HTTP response arrived now.

Do not implement direct recovery by recursively calling `_refresh_once()`.

### Shared-cache isolation

Audit `_SHARED_CACHE`, currently process-global and symbol-keyed.

If two clients can use distinct data transports, sandbox/live endpoints, credentials, or provider semantics, use a stable namespace such as:

```text
market_data_source_id | symbol
```

The namespace must not include secrets or raw tokens. A stable source ID may be derived from broker class, normalized base URL, environment, and execution mode.

A PAPER cache entry must never satisfy a LIVE protective recovery request unless both are proven to use the same canonical market-data source.

## Required provider-time contract

Reuse #385's canonical timestamp normalizer after rebase.

For every option and underlying quote, preserve:

```text
provider_timestamp
receipt_timestamp
source
provenance
```

Rules:

- provider time establishes market freshness;
- receipt time establishes when the service received the payload;
- missing provider time is not silently equivalent to current market time;
- excessive future skew fails closed;
- a stale provider timestamp remains stale after a new HTTP fetch;
- health/severity clears only after genuinely fresh provider truth is applied;
- do not create a second incompatible timestamp parser in this PR.

## Required recovery state machine

Use a small explicit state machine. Required states:

```text
MONITORED
STALE_DETECTED
REFRESH_PENDING
REFRESH_IN_PROGRESS
RECOVERED
BACKOFF_DEFERRED
RECOVERY_EXHAUSTED
ADOPTION_MISSING
MONITOR_STALLED
BINDING_INVALID
```

Required transitions:

1. fresh provider truth applied -> `MONITORED`;
2. stale threshold crossed -> `STALE_DETECTED`;
3. direct fetch scheduled -> `REFRESH_PENDING`;
4. direct fetch starts -> `REFRESH_IN_PROGRESS`;
5. fresh truth applied to exact position and engine woken -> `RECOVERED`, then `MONITORED`;
6. 429/global backoff -> `BACKOFF_DEFERRED` with `next_recovery_at`;
7. bounded attempts exhausted -> `RECOVERY_EXHAUSTED` and critical event;
8. position not seen by QPM within one configured cycle after adoption -> `ADOPTION_MISSING`;
9. loop heartbeat stalled with open LIVE positions -> `MONITOR_STALLED`;
10. client/mode/broker/engine binding mismatch -> `BINDING_INVALID`.

Do not clear a critical state merely because another cycle ran. Clear it only after the required fresh evidence is applied.

## Recovery limits

Use environment-configurable constants with conservative defaults. Define them once, not with repeated `os.getenv()` calls throughout loops.

Suggested names:

```text
LIVE_QPM_ADOPTION_MAX_SEC
LIVE_QPM_STALE_RECOVERY_ATTEMPTS
LIVE_QPM_RECOVERY_RETRY_SEC
LIVE_QPM_RECOVERY_MAX_BACKOFF_SEC
LIVE_QPM_HEARTBEAT_STALL_SEC
LIVE_QPM_CRITICAL_ALERT_COOLDOWN_SEC
```

Requirements:

- no more than one direct recovery attempt per position per configured interval;
- retries must be bounded before critical escalation;
- controlled retries may continue after escalation at a slower cadence;
- global/provider 429 backoff remains authoritative;
- one position's recovery must not starve ordinary polling for other positions;
- no busy loop and no sleep while holding the exit-engine lock.

## QPM lifecycle recovery

`ClientRunner` owns QPM lifecycle.

Add one lifecycle lock and monotonic generation counter to prevent concurrent health checks from spawning duplicate monitors.

Before reusing an existing monitor, verify:

```text
existing.client_id == runner.email
existing execution_mode == runner.mode
existing.broker is the resolved data broker
existing.exit_engine is the runner's current exit engine
existing thread is alive
existing loop heartbeat is advancing
```

If an existing monitor is alive but incorrectly bound, treat it as invalid. Stop and join it before replacement.

If the thread is dead or heartbeat-stalled while open LIVE positions exist:

1. acquire lifecycle lock;
2. re-check under the lock;
3. stop/join the prior generation best-effort;
4. create exactly one replacement generation;
5. attach it to the current exit engine;
6. start it;
7. kick it immediately;
8. emit one recovery event containing old/new instance IDs and generation.

Do not create duplicate `ExitReliabilityMonitor` instances during QPM-only restart. Its lifecycle must remain independently idempotent.

Runner entry gating may remain blocked while QPM coverage is unhealthy, but exit monitoring, emergency flatten, reconciliation, and EOD risk reduction must remain alive.

## Per-client health registry

Register QPM under a unique non-secret key, for example:

```text
ap_quote_monitor:<normalized-client-hash>:<execution-mode>
```

or the repository's existing safe equivalent.

Metrics must include:

```text
monitor_instance_id
thread_generation
cycle_attempt_age_sec
cycle_success_age_sec
consecutive_cycle_failures
positions_expected
positions_seen
positions_fresh
positions_stale
positions_recovery_exhausted
```

One client's heartbeat must not make another client's monitor healthy.

## Critical events and reason codes

Use stable codes. At minimum:

```text
LIVE_POSITION_MONITOR_ADOPTED
LIVE_POSITION_MONITOR_ADOPTION_MISSING
LIVE_POSITION_QUOTE_STALE
LIVE_POSITION_PROVIDER_DATA_STALE
LIVE_POSITION_REFRESH_REQUESTED
LIVE_POSITION_REFRESH_BACKOFF_DEFERRED
LIVE_POSITION_REFRESH_SUCCEEDED
LIVE_POSITION_REFRESH_FAILED
LIVE_POSITION_REFRESH_EXHAUSTED
LIVE_POSITION_MONITOR_HEARTBEAT_STALLED
LIVE_POSITION_MONITOR_RESTARTED
LIVE_POSITION_MONITOR_BINDING_INVALID
LIVE_POSITION_SNAPSHOT_APPLY_FAILED
LIVE_POSITION_EXIT_ENGINE_WAKE_FAILED
```

A critical exhausted event must include:

```text
client_id
execution_mode
position_id
option_symbol
underlying
monitor_instance_id
thread_generation
option_provider_age_sec
underlying_provider_age_sec
option_receipt_age_sec
underlying_receipt_age_sec
cycle_attempt_age_sec
cycle_success_age_sec
consecutive_cycle_failures
consecutive_position_failures
recovery_generation
recovery_attempt_count
last_failure_code
last_broker_error
shared_backoff_remaining_sec
commit_sha
pod_id
```

Do not log broker credentials, tokens, or full account secrets.

## Operator severity contract

Expose one canonical position-monitoring record through the existing operator health surface.

Severity must distinguish:

- `fresh`;
- `stale_recovery_pending`;
- `stale_provider_data`;
- `adoption_missing`;
- `monitor_heartbeat_stalled`;
- `broker_failed`;
- `rate_limited_backoff`;
- `binding_invalid`;
- `recovery_exhausted`.

The stale banner must not clear because the thread is alive or because a request completed. It clears only after fresh provider truth reached the exact exit-engine position.

## Required production code limits

Expected files:

```text
ap/position_quote_monitor.py
client_runner.py
ap_exit_engine.py                  # narrow adoption/attachment/wake hooks only
ap_health_registry.py              # only if unique registration needs support
app.py                             # existing health response only
.github/workflows/p0_regression.yml
tests/test_p0_live_position_quote_monitor_recovery.py
```

Any additional production file must be justified in the PR description before merge.

Do not modify:

```text
contract selector
signal pipeline
intelligence admission
risk manager
entry watcher trigger policy
position sizing
BUY submission pricing
proof_trades
exit thresholds
```

## Required tests

Tests must call real production methods. Mock only external broker transport, persistence, alert delivery, process clock, and thread scheduling where unavoidable.

Do not reproduce production formulas in tests and then test the copied formula.

### Adoption and identity

1. New LIVE fill appears in `active_positions()`, kicks QPM, and is polled within one cycle.
2. Broker-repaired LIVE position is polled within one cycle.
3. Restart-hydrated LIVE position starts invalid and becomes monitored only after fresh truth.
4. Two positions with the same contract retain separate monitoring and recovery state.
5. Reopened contract with a new position ID starts a new generation.
6. Missing position ID fails closed and emits identity-critical evidence.
7. Wrong client binding fails closed.
8. Wrong execution-mode binding fails closed.
9. Wrong exit-engine binding causes replacement, not silent reuse.

### Cycle and heartbeat truth

10. Thread alive plus successful cycles reports healthy.
11. Thread alive plus repeated broker exceptions reports cycle failure, not healthy coverage.
12. `_last_cycle_success_ts` does not advance on failed cycles.
13. One client's heartbeat does not update another client's health-registry key.
14. QPM heartbeat stall with an open LIVE position triggers exactly one restart generation.
15. Two concurrent health checks do not create duplicate QPM threads.
16. QPM-only restart does not duplicate `ExitReliabilityMonitor`.

### Quote and cache truth

17. Successful quote followed by missing BID invalidates current-cycle executable truth and schedules recovery.
18. Stale option provider timestamp remains stale despite current receipt time.
19. Stale underlying provider timestamp remains stale despite current receipt time.
20. Missing provider timestamp does not become fresh by substitution with `now`.
21. Future-skewed provider timestamp fails closed.
22. Direct recovery bypasses cached payload.
23. Direct recovery respects global 429 backoff and records `BACKOFF_DEFERRED`.
24. LIVE recovery cannot consume a differently namespaced PAPER or sandbox cache entry.
25. Ordinary same-source cache behavior remains functional.

### Recovery and exact-object delivery

26. Stale detection schedules one position-scoped recovery generation.
27. Successful direct recovery applies to a separate consumer object matched by position ID.
28. Consumer receives raw provider timestamps, receipt timestamps, validity, and provenance.
29. Successful recovery wakes exit evaluation even when price movement is below ordinary wake threshold.
30. Snapshot apply failure remains critical and does not clear stale severity.
31. Exit-engine wake failure remains critical and does not clear stale severity.
32. Broker error increments bounded attempts while the position remains visible.
33. Recovery exhaustion emits one complete critical event and then retries at controlled cadence.
34. A later genuine fresh quote clears exhausted severity and returns the state to monitored.
35. A merely repeated stale payload does not clear exhausted severity.

### Safety and compatibility

36. No quote failure automatically flattens a position.
37. Emergency flatten remains reachable.
38. EOD force close remains reachable.
39. PAPER behavior remains isolated.
40. Existing #385 soft-exit stale-deferral tests remain green.
41. Existing #385 hard-exit authority tests remain green.
42. Existing broker-open protective monitoring tests remain green.
43. Runner shutdown stops the current QPM generation without leaving a duplicate thread.

## Focused test command

At minimum, execute and report:

```bash
pytest -q \
  tests/test_p0_live_position_quote_monitor_recovery.py \
  tests/test_soft_exit_executable_truth.py \
  tests/test_p0_broker_open_protective_monitoring.py \
  tests/test_p0_live_executable_bid_pnl.py \
  tests/test_client_runner_hardening.py
```

Then run the exact P0 workflow command from `.github/workflows/p0_regression.yml` against the final head.

Do not report “tests should pass.” Report the exact commands, counts, failures, and final head SHA.

## Controlled proof

Before merge, provide one controlled LIVE or production-equivalent trace showing:

```text
broker fill or broker-open discovery
→ exit-engine position adoption
→ QPM position first seen
→ option and underlying poll attempted
→ raw provider timestamps evaluated
→ fresh snapshot applied to exact ManagedPosition
→ exit engine woken
→ operator health reports fresh monitoring
```

Include position ID, redacted client identity, option symbol, monitor instance ID, thread generation, timestamps, and commit SHA. Do not expose tokens or account secrets.

## Acceptance criteria

This PR is complete only when all are proven:

- every broker-open LIVE position is represented in exit-engine and QPM monitoring state;
- newly adopted positions are attempted within one configured cycle;
- service liveness and position quote coverage are separately observable;
- repeated failed cycles cannot report healthy coverage;
- one client's heartbeat cannot mask another client's failure;
- stale severity automatically initiates bounded recovery;
- direct recovery bypasses stale cached payloads without bypassing rate-limit safety;
- stale provider data cannot be relabeled fresh from receipt time;
- successful recovery reaches the exact position used for exit decisions and wakes evaluation;
- dead, detached, or incorrectly bound QPM instances are detected and replaced exactly once;
- recovery exhaustion produces actionable critical evidence while keeping the position visible;
- existing emergency, EOD, reconciliation, and #385 safety behavior remains intact;
- no entry rules or exit thresholds are changed.

## Merge gates

- Branch rebased after #385 and preceding trade-flow PRs.
- Root-cause investigation note posted.
- Final diff limited to justified files.
- Exact-head focused tests green.
- Exact-head P0 workflow green.
- Controlled proof attached.
- PR remains draft until review.
- No merge without explicit `merge #387` approval from Angel.
