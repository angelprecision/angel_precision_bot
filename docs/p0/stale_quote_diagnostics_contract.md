# P0 — Stale Quote Diagnostics Contract

> Restored after PR #354 removed it. Originally introduced in PR #321;
> removed as collateral when PR #321 runtime code was reverted. No runtime
> behavior was changed by PR #354. This file restores the acceptance contract
> against current `main` architecture.

---

## Goal

Identify exactly when and why an active broker-open position becomes stale,
without changing broker submit/cancel behavior or existing protective-exit
policy.

---

## Scope

Primary files governed by this contract:

- `ap/position_quote_monitor.py`
- `ap_exit_engine.py`
- `ap_reconciler.py`
- `client_runner.py`
- `ap/exit_fill_truth_guard.py` (added post-PR #321 -- stale-attempt lease)
- tests covering quote health and exit decisions

---

## Non-goals

Any PR touching stale quote handling must not:

- change stop-loss thresholds
- change take-profit thresholds
- change broker order type or price construction
- submit or cancel any additional broker order
- mutate orders, positions, proof_trades, or queue lifecycle beyond additive diagnostics
- change client_id or execution_mode
- change paper/live taxonomy

---

## Quote health taxonomy -- QPM quote_state

APPositionQuoteMonitor._classify_health() sets pos.quote_state to exactly
one of these four strings on every cycle (ap/position_quote_monitor.py,
_classify_health):

| quote_state | Condition |
|---|---|
| fresh | opt_age <= QUOTE_FRESH_SEC (default 3 s) |
| degraded | opt_age <= QUOTE_DEGRADED_SEC (default 8 s) |
| stale | opt_age <= QUOTE_STALE_SEC (default 15 s) |
| blind | opt_age > QUOTE_STALE_SEC -- assigned immediately; QUOTE_BLIND_ALERT_CYCLES gates alerting only, not state assignment |

These four strings are the only values written to pos.quote_state in
production. A PR must not introduce a fifth string without updating this
table and the exit-engine stale check.

### QPM non-state reasons and metrics (not quote_state)

The following appear in QPM but are not persisted as quote_state:

| Identifier | Where | What |
|---|---|---|
| "missing_contract" | Return value of _build_terminal_query() when contract symbol is blank | Locator failure reason; internal to QPM; never set on pos.quote_state |
| "rate_limited" | self._metrics["rate_limited"] counter; _handle_429() | Backoff metric; backoff wall time is in _SHARED_BACKOFF_UNTIL; never set on pos.quote_state |

A future PR that surfaces either of these as a quote_state value must
update this contract and the taxonomy table above.

---

## Exit-engine stale reason codes (separate layer from QPM quote_state)

ap_exit_engine._is_option_quote_stale() returns a (stale: bool, age: float
or None, reason: str) tuple. These reason codes are a distinct classification
layer from pos.quote_state set by QPM:

| Reason code | Meaning |
|---|---|
| fresh_option_quote | age <= STALE_OPTION_QUOTE_MAX_AGE_SEC |
| stale_option_quote | age > STALE_OPTION_QUOTE_MAX_AGE_SEC |
| missing_option_quote | pos.last_option_quote_update_ts is None |
| invalid_option_quote_ts | Timestamp present but age computation raises |

Any PR touching exit-engine stale logic must not conflate these codes with
the QPM quote_state strings -- they are separate classification layers.
A PR must not assign an exit-engine reason code to pos.quote_state or
assign a QPM quote_state string as an exit-engine reason code.

---

## States not yet implemented in production

Required before any PR that adds classification logic in those paths:

| State | Path | Required before |
|---|---|---|
| broker_quote_error | QPM HTTP non-429 error path | Adding per-position broker quote error diagnostics |
| position_not_seeded | Exit-engine seeding path | Adding seeding-gap detection |
| quote_monitor_not_running | QPM thread-health path | Adding thread-health diagnostics |

---

## Required diagnostic payload

For each active broker-open position, the diagnostic surface must expose:

### Identity
- position_id
- client_id
- execution_mode
- contract
- underlying

### Quantity
- broker_open_qty
- db_quantity_remaining
- exit_engine_quantity_remaining

### Quote health (QPM layer)
- quote_state (one of the four QPM taxonomy strings)
- quote_state_reason (free-text detail)
- last_option_quote_update_ts
- last_underlying_quote_update_ts
- option_quote_age_sec
- underlying_quote_age_sec

### Quote monitor internals
- last_quote_attempt_ts
- last_quote_success_ts
- last_quote_error
- last_quote_http_status
- rate_limit_backoff_until
- qpm_thread_alive
- qpm_last_cycle_age_sec

### Thread health
- exit_engine_thread_alive
- reconciler_thread_alive

### Exit decision (exit-engine layer)
- last_exit_decision_code
- last_exit_decision_suppressed
- last_exit_suppression_reason

---

## Required behavioral invariants

1. Quote polling continues for every exit-engine active position
   regardless of wake-cooldown state.
2. Wake cooldown may suppress only an event-driven wake, never quote
   polling itself.
3. Stale classification must not alter protective exit behavior in any
   PR that only adds diagnostics.
4. A broker-open position absent from exit-engine memory must emit
   position_not_seeded (not yet implemented -- required before adding
   seeding-gap diagnostics).
5. A dead or stalled QPM thread must emit quote_monitor_not_running
   (not yet implemented -- required before adding thread-health diagnostics).
6. Rate limiting must be distinguishable from missing/invalid contract
   data in any future diagnostic that surfaces these conditions.
7. Diagnostics must preserve exact client_id and execution_mode.
8. Diagnostics must be additive and best-effort -- failure to write
   telemetry must never block exits.
9. QPM quote_state and exit-engine stale reason codes must not be
   conflated. They are separate classification layers.
10. exit_fill_truth_guard stale-attempt lease (_RECONCILIATION_STALE_ATTEMPT_LEASE,
    default 5 min) is a write-lock staleness concept scoped to reconciliation
    retry windows and must not be conflated with position quote staleness.

---

## Architecture note -- post-PR #321 additions

The following were added after PR #321 was originally written and must be
considered in any implementation that extends this contract:

- ap/exit_fill_truth_guard.py: introduced stale reconciliation attempt lease
  (_RECONCILIATION_STALE_ATTEMPT_LEASE = 5 min). This is a write-lock
  staleness concept, not a quote-freshness concept. Keep these two
  staleness definitions separate.
- STALE_EXIT_RETRY_* constants in ap_exit_engine.py: stale exit decision
  retry window (max attempts, max age, delay). Separate from QPM quote age.
- ap/fill_monitor.py, ap/trade_lifecycle_guards.py: EXIT fill reconciliation
  paths introduced in PR #360. Any stale-quote PR must verify these paths
  still receive accurate quote_state from QPM.

---

## Acceptance checklist

Use this checklist for any PR that touches:
- APPositionQuoteMonitor or ap/position_quote_monitor.py
- exit-engine stale-quote branch in ap_exit_engine.py
- ap_reconciler.py stale-exit detection
- QPM memory, thread lifecycle, or backoff logic
- ap/exit_fill_truth_guard.py reconciliation-stale logic
- recovery paths that read quote_state off positions

- [ ] pos.quote_state is one of: fresh, degraded, stale, blind
- [ ] blind is assigned when opt_age > QUOTE_STALE_SEC, not gated on BLIND_ALERT_CYCLES
- [ ] Exit-engine stale reason codes are not assigned to pos.quote_state
- [ ] QPM quote_state strings are not used as exit-engine reason codes
- [ ] Diagnostic payload fields are additive -- no removals
- [ ] client_id and execution_mode preserved on every diagnostic write
- [ ] No broker submit/cancel introduced
- [ ] No stop-loss or take-profit threshold change
- [ ] No mutation to orders, positions, proof_trades, or queue beyond diagnostics
- [ ] Thread-health fields (qpm_thread_alive, exit_engine_thread_alive) remain accurate
- [ ] Failure to write diagnostics does not block exits
- [ ] PR #360 EXIT fill paths (fill_monitor.py, trade_lifecycle_guards.py)
      verified to receive accurate quote_state if quote paths are touched
