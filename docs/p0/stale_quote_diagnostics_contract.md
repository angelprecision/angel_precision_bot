# P0 — Stale Quote Diagnostics Contract

> **Restored by PR #365** — originally introduced in PR #321, removed with
> the PR #321 runtime revert (PR #354). No runtime behavior was changed by
> PR #354; only this acceptance contract was lost. This file restores it
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
- `ap/exit_fill_truth_guard.py` *(added post-PR #321 — stale-attempt lease)*
- tests covering quote health and exit decisions

---

## Non-goals

Any PR touching stale quote handling must not:

- change stop-loss thresholds
- change take-profit thresholds
- change broker order type or price construction
- submit or cancel any additional broker order
- mutate `orders`, `positions`, `proof_trades`, or queue lifecycle beyond
  additive diagnostics
- change `client_id` or `execution_mode`
- change paper/live taxonomy

---

## Required stale taxonomy

Every active position quote cycle must resolve to exactly one state.
Current production strings (lowercase, as emitted by `APPositionQuoteMonitor`):

| State string | Meaning |
|---|---|
| `fresh` | Option quote ≤ `QUOTE_FRESH_SEC` (default 3 s) |
| `degraded` | Option quote ≤ `QUOTE_DEGRADED_SEC` (default 8 s) |
| `stale` | Option quote ≤ `QUOTE_STALE_SEC` (default 15 s) |
| `blind` | Option quote older than stale threshold for ≥ `QUOTE_BLIND_ALERT_CYCLES` cycles |
| `missing_contract` | Contract not found in quote monitor; no quote available |
| `rate_limited` | QPM is in rate-limit backoff; quote attempt suppressed |

**States not yet implemented in production** (required before any PR that adds
classification logic in these paths):

| State string | Meaning |
|---|---|
| `broker_quote_error` | HTTP error from broker quote API (non-rate-limit) |
| `position_not_seeded` | Position absent from exit-engine memory |
| `quote_monitor_not_running` | QPM thread dead or stalled beyond `QUOTE_HEARTBEAT_DEGRADED_SEC` |

Generic `stale` without a specific cause is not sufficient.
A PR may not collapse `missing_contract`, `rate_limited`, or
`broker_quote_error` into a single unclassified `stale`.

---

## Required diagnostic payload

For each active broker-open position, the diagnostic surface must expose:

### Identity
- `position_id`
- `client_id`
- `execution_mode`
- `contract`
- `underlying`

### Quantity
- `broker_open_qty`
- `db_quantity_remaining`
- `exit_engine_quantity_remaining`

### Quote health
- `quote_state` *(one of the taxonomy strings above)*
- `quote_state_reason` *(free-text detail, e.g. contract symbol, HTTP status)*
- `last_option_quote_update_ts`
- `last_underlying_quote_update_ts`
- `option_quote_age_sec`
- `underlying_quote_age_sec`

### Quote monitor internals
- `last_quote_attempt_ts`
- `last_quote_success_ts`
- `last_quote_error`
- `last_quote_http_status`
- `rate_limit_backoff_until`
- `qpm_thread_alive`
- `qpm_last_cycle_age_sec`

### Thread health
- `exit_engine_thread_alive`
- `reconciler_thread_alive`

### Exit decision
- `last_exit_decision_code`
- `last_exit_decision_suppressed`
- `last_exit_suppression_reason`

---

## Required behavioral invariants

1. **Quote polling continues** for every exit-engine active position regardless
   of wake-cooldown state.
2. **Wake cooldown** may suppress only an event-driven wake, never quote
   polling itself.
3. **Stale classification must not alter protective exit behavior** in any PR
   that only adds diagnostics.
4. **A broker-open position absent from exit-engine memory** must emit
   `position_not_seeded` (not yet implemented — required before adding
   seeding-gap diagnostics).
5. **A dead or stalled QPM thread** must emit `quote_monitor_not_running`
   (not yet implemented — required before adding thread-health diagnostics).
6. **Rate limiting must be distinguishable** from missing/invalid contract
   data (`rate_limited` vs `missing_contract`). ✅ *Implemented.*
7. **Diagnostics must preserve exact `client_id` and `execution_mode`.**
8. **Diagnostics must be additive and best-effort** — failure to write
   telemetry must never block exits.
9. **`exit_fill_truth_guard` stale-attempt lease** (`_RECONCILIATION_STALE_ATTEMPT_LEASE`,
   default 5 min) is a separate staleness concept scoped to reconciliation
   retry windows; it must not be conflated with position quote staleness.

---

## Acceptance checklist

Use this checklist for any PR that touches:

- `APPositionQuoteMonitor` or `ap/position_quote_monitor.py`
- exit-engine stale-quote branch in `ap_exit_engine.py`
- `ap_reconciler.py` stale-exit detection
- QPM memory, thread lifecycle, or backoff logic
- `ap/exit_fill_truth_guard.py` reconciliation-stale logic
- recovery paths that read `quote_state` off positions

Checklist:

- [ ] Quote taxonomy is one of the defined state strings (no unclassified `stale`)
- [ ] `missing_contract` and `rate_limited` remain distinguishable
- [ ] Diagnostic payload fields are additive — no removals
- [ ] `client_id` and `execution_mode` preserved on every diagnostic write
- [ ] No broker submit/cancel introduced
- [ ] No stop-loss or take-profit threshold change
- [ ] No mutation to `orders`, `positions`, `proof_trades`, or queue beyond diagnostics
- [ ] Thread-health fields (`qpm_thread_alive`, `exit_engine_thread_alive`) remain accurate
- [ ] Failure to write diagnostics does not block exits

---

## Architecture note — post-PR #321 additions

The following were added after PR #321 was originally written and must be
considered in any implementation that extends this contract:

- **`ap/exit_fill_truth_guard.py`** — introduced stale reconciliation attempt
  lease (`_RECONCILIATION_STALE_ATTEMPT_LEASE = 5 min`). This is a
  write-lock staleness concept, not a quote-freshness concept. Do not merge.
- **`STALE_EXIT_RETRY_*` constants in `ap_exit_engine.py`** — stale exit
  decision retry window (max attempts, max age, delay). Separate from QPM
  quote age.
- **`ap/fill_monitor.py`**, **`ap/trade_lifecycle_guards.py`** — EXIT fill
  reconciliation paths introduced in PR #360. Any stale-quote PR must verify
  these paths still receive accurate `quote_state` from QPM.
