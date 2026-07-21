# P0: Restore fresh quote monitoring and automatic recovery for LIVE open positions

## Status

Draft implementation specification only. Do not merge until PR #385 and the preceding trade-flow fixes are complete and stable.

## Production incident

On July 21, 2026, Jason's LIVE account opened a position, but the operator severity view reported the position quote as stale. The bot was not actively managing the position from fresh market truth, and the operator manually exited it.

This is a live-money monitoring failure. A stale warning is not sufficient if the system does not recover the quote stream or escalate that protective monitoring is unavailable.

## Relationship to PR #385

PR #385 defines what exit decisioning may do when option or underlying truth is stale, missing, or non-executable. It must remain separate.

This PR addresses the upstream operational failure:

> Why did an open LIVE position remain stale instead of being continuously polled, refreshed, recovered, or escalated?

Do not copy PR #385's exit-policy work into this branch. This PR must restore quote-monitor coverage and liveness so PR #385 receives fresh truth whenever the broker can provide it.

## Scope

Trace and harden the full LIVE open-position monitoring path:

1. broker fill or broker-truth discovery;
2. position adoption by `APExitEngine`;
3. visibility through `active_positions()`;
4. adoption by `APPositionQuoteMonitor`;
5. inclusion in every applicable quote batch;
6. per-cycle quote success or explicit failure;
7. stale classification;
8. immediate single-position recovery;
9. monitor/thread recovery when polling has stopped;
10. critical escalation when fresh truth cannot be restored.

## Non-goals

- Do not redesign the exit engine.
- Do not alter take-profit, stop-loss, trail, or hard-stop thresholds.
- Do not rewrite QuoteAuthority.
- Do not modify entry-selection rules.
- Do not add another general readiness framework.
- Do not silently flatten a position solely because one quote cycle failed.
- Do not weaken PR #385's requirement that soft exits use fresh executable BID truth.

A separate entry-side incident is required only if the quote used for the original LIVE broker submit was itself stale or the final LIVE submit gate was bypassed.

## Required investigation before coding

For the July 21 Jason position, reconstruct this exact timeline from durable evidence:

- fill timestamp;
- position row creation/update timestamp;
- exit-engine adoption timestamp;
- first QPM poll attempt;
- first successful option quote;
- first successful underlying quote;
- last successful quote before severity became stale;
- every refresh request after staleness;
- QPM thread/heartbeat status;
- broker response or exception for each failed cycle;
- whether shared cache, rate-limit backoff, symbol normalization, or position identity caused the position to be skipped;
- whether `apply_quote_snapshots()` reached the same `ManagedPosition` used by exit evaluation;
- whether more than one QPM or exit-engine instance existed for the same LIVE client.

Do not begin with a guessed architecture change. Identify the first broken seam in this timeline.

## Required production behavior

### 1. Deterministic position adoption

A newly filled or broker-discovered LIVE position must become visible to the client's QPM within one polling cycle.

Use durable position identity, not contract symbol alone:

```text
client_id | execution_mode | position_id | option_symbol
```

The monitor must never skip a LIVE position because an in-memory list was captured before the fill, a restart lost local state, or the same contract appeared previously.

### 2. Per-position polling state

Track explicit per-position monitoring state, at minimum:

```text
last_poll_attempt_ts
last_poll_success_ts
last_option_success_ts
last_underlying_success_ts
consecutive_failed_cycles
consecutive_stale_cycles
last_failure_code
recovery_state
recovery_attempt_count
last_recovery_attempt_ts
monitor_instance_id
```

A generic service heartbeat is not enough. The service may be alive while one client's position is silently omitted.

### 3. Stale state must cause recovery, not only classification

When a LIVE open position crosses the stale threshold:

1. mark the position stale with a stable reason code;
2. immediately issue a direct single-contract option quote refresh and a direct underlying refresh;
3. bypass a stale shared-cache value for that recovery attempt;
4. preserve raw provider timestamps and provenance;
5. publish the recovered snapshot to the exit engine;
6. wake exit evaluation;
7. clear stale severity only after a real fresh quote is applied.

Suggested reason codes:

```text
LIVE_POSITION_QUOTE_STALE
LIVE_POSITION_REFRESH_REQUESTED
LIVE_POSITION_REFRESH_SUCCEEDED
LIVE_POSITION_REFRESH_FAILED
LIVE_POSITION_MONITOR_ADOPTION_MISSING
LIVE_POSITION_MONITOR_HEARTBEAT_STALLED
LIVE_POSITION_MONITOR_RECOVERY_EXHAUSTED
```

### 4. Detect a dead or detached monitor

Add a liveness check that distinguishes:

- service alive, position polled successfully;
- service alive, position omitted;
- poll attempted, broker failed;
- poll attempted, provider returned stale data;
- QPM loop/thread stopped advancing;
- wrong monitor or wrong client binding.

If the QPM loop stops advancing while LIVE positions remain open, restart or rebind the monitor through the existing service lifecycle exactly once per recovery generation. Prevent duplicate monitor threads.

### 5. Broker-truth reconciliation must reattach monitoring

When broker precheck discovers a position missing from the engine, loading it into `APExitEngine` is not enough. The same repair must guarantee that QPM observes it on the next cycle and records a monitoring-adoption event.

### 6. Bounded recovery and critical escalation

A position must never remain silently stale indefinitely.

After bounded direct-refresh attempts fail:

- keep the position visible;
- keep existing risk-reducing emergency/EOD paths available;
- emit a critical operator event identifying the exact client, position, contract, quote ages, broker error, QPM heartbeat age, and recovery attempts;
- continue retrying at a controlled cadence without exhausting broker rate limits;
- never relabel stale data as fresh merely because it was fetched again.

Automatic flattening due only to unavailable quotes must remain a separate explicit policy decision, not an accidental side effect of this PR.

## Observability requirements

Every LIVE open position must expose one canonical monitoring record containing:

```text
client_id
position_id
option_symbol
monitor_instance_id
poll_attempt_age_sec
option_quote_age_sec
underlying_quote_age_sec
consecutive_failed_cycles
consecutive_stale_cycles
recovery_state
last_failure_code
last_broker_error
last_refresh_result
```

The severity tab must distinguish:

- stale but recovery in progress;
- stale because broker returned old provider data;
- stale because the position was not adopted;
- stale because the QPM heartbeat stopped;
- stale because broker requests failed or were rate limited;
- recovery exhausted and operator action required.

## Tests

Tests must call real production methods and mock only external boundaries such as broker transport, persistence, alert delivery, and clock.

Required cases:

1. A new LIVE fill is adopted by QPM within one cycle.
2. A broker-repaired position is adopted by QPM within one cycle.
3. A position cannot be skipped because another position uses the same contract.
4. A successful poll followed by missing BID marks the current cycle invalid and requests recovery.
5. A stale provider timestamp remains stale even when the HTTP fetch occurred now.
6. A direct recovery fetch bypasses stale shared cache.
7. A direct recovery success applies a fresh snapshot and wakes exit evaluation.
8. A broker error increments bounded recovery state and preserves visibility.
9. A stalled QPM heartbeat with open LIVE positions triggers one monitor recovery generation.
10. Recovery does not create duplicate QPM threads or duplicate snapshots.
11. Wrong-client or wrong-mode monitor binding fails closed and raises a critical event.
12. Separate producer and exit-engine consumer objects receive identical quote truth and timestamps.
13. Severity remains critical after recovery exhaustion and cannot clear without a real fresh quote.
14. PAPER behavior remains isolated and does not weaken LIVE guarantees.
15. Existing PR #385 stale-quote deferral and hard-exit tests remain green.

## Acceptance criteria

This PR is complete only when all of the following are proven:

- Every broker-open LIVE position is represented in exit-engine and QPM monitoring state.
- New positions are polled within one configured polling cycle.
- Stale severity initiates active recovery automatically.
- A successful recovery reaches the exact `ManagedPosition` evaluated for exits.
- A dead or detached monitor is detected without waiting for an operator to notice the dashboard.
- Stale data cannot be relabeled fresh from receipt time alone.
- Recovery is bounded, rate-limit aware, idempotent, and observable.
- Failure to recover produces a critical event with actionable evidence.
- No production exit thresholds or entry rules are broadened.

## Merge gates

- Draft only until PR #385 is merged and stable.
- Exact-head focused tests green.
- Existing P0 suite green.
- One controlled LIVE or production-equivalent proof demonstrates fill → QPM adoption → fresh snapshot → exit-engine wake.
- No merge without explicit approval from Angel.
