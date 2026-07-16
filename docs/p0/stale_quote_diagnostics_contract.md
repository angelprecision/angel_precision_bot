# P0 — Stale Quote Diagnostics Contract

## Goal

Identify exactly when and why an active broker-open position becomes stale, without changing broker submit/cancel behavior or existing protective-exit policy.

## Scope

Primary files to amend in the implementation PR:

- `ap/position_quote_monitor.py`
- `ap_exit_engine.py`
- `ap_reconciler.py`
- `client_runner.py`
- tests covering quote health and exit decisions

## Non-goals

This PR must not:

- change stop-loss thresholds
- change take-profit thresholds
- change broker order type or price construction
- submit or cancel any additional broker order
- mutate `orders`, `positions`, `proof_trades`, or queue lifecycle beyond additive diagnostics
- change `client_id` or `execution_mode`
- change paper/live taxonomy

## Required stale taxonomy

Every active position quote cycle must resolve to exactly one state:

- `FRESH`
- `DEGRADED`
- `STALE`
- `BLIND`
- `MISSING_CONTRACT`
- `BROKER_QUOTE_ERROR`
- `RATE_LIMITED`
- `QUOTE_MONITOR_NOT_RUNNING`
- `POSITION_NOT_SEEDED`

Generic `stale` without a specific cause is not sufficient.

## Required diagnostic payload

For each active broker-open position, expose:

- `position_id`
- `client_id`
- `execution_mode`
- `contract`
- `underlying`
- `broker_open_qty`
- `db_quantity_remaining`
- `exit_engine_quantity_remaining`
- `quote_state`
- `quote_state_reason`
- `last_option_quote_update_ts`
- `last_underlying_quote_update_ts`
- `option_quote_age_sec`
- `underlying_quote_age_sec`
- `last_quote_attempt_ts`
- `last_quote_success_ts`
- `last_quote_error`
- `last_quote_http_status`
- `rate_limit_backoff_until`
- `qpm_thread_alive`
- `qpm_last_cycle_age_sec`
- `exit_engine_thread_alive`
- `reconciler_thread_alive`
- `last_exit_decision_code`
- `last_exit_decision_suppressed`
- `last_exit_suppression_reason`

## Required behavior

1. Quote polling continues for every exit-engine active position.
2. Wake cooldown may suppress only an event-driven wake, never quote polling.
3. Stale classification must not alter protective exit behavior in this PR.
4. A broker-open position missing from exit-engine memory must emit `POSITION_NOT_SEEDED`.
5. A dead or stalled quote-monitor thread must emit `QUOTE_MONITOR_NOT_RUNNING`.
6. Rate limiting must be distinguishable from missing/invalid contract data.
7. Diagnostics must preserve exact `client_id` and `execution_mode`.
8. Diagnostics must be additive and best-effort; failure to write telemetry must never block exits.

## Required metrics

- active positions tracked
- fresh/degraded/stale/blind counts
- stale duration histogram
- quote fetch failures by reason
- rate-limit count and backoff duration
- positions missing from exit-engine memory
- stale decisions generated
- stale decisions suppressed
- forced-risk exits allowed through stale mode

## Required tests

1. OPEN live position + fresh quote -> `FRESH`.
2. OPEN live position + aged quote -> `STALE` with exact age.
3. OPEN live position + no quote ever -> `BLIND`.
4. OPEN broker position absent from engine -> `POSITION_NOT_SEEDED`.
5. QPM thread dead -> `QUOTE_MONITOR_NOT_RUNNING`.
6. HTTP 429 -> `RATE_LIMITED`, not generic stale.
7. Missing contract symbol -> `MISSING_CONTRACT`.
8. Wake cooldown increments `wakes_suppressed` but regular poll continues.
9. Telemetry write failure does not block stop-loss or emergency exit evaluation.
10. Diagnostics preserve live/paper and client identity.

## Acceptance

This PR is mergeable only when stale quote origin can be identified from one diagnostic record without reconstructing logs across multiple services.
