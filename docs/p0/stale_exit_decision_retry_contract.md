# P0 — Bounded Stale Exit Decision Retry Contract

## Goal

Preserve currently working protective exits while ensuring a stale quote cannot cause an actionable exit decision to disappear into an unowned `continue` path.

## Dependency

Implement after the stale quote diagnostics PR so retry behavior can use an exact stale reason instead of generic quote failure.

## Scope

Primary implementation files:

- `ap_exit_engine.py`
- `ap/position_quote_monitor.py`
- `ap/order_state_machine.py` only if durable exit-retry ownership belongs there
- `ap_reconciler.py`
- tests for exit decision suppression and retry

## Non-goals

This PR must not:

- relax hard-stop, EOD, sentinel, or emergency-exit behavior
- change broker submit/cancel pricing policy
- convert stale quotes into guessed fresh prices
- create duplicate exit orders
- mutate entry orders, entry queue state, or `proof_trades`
- alter `client_id` or `execution_mode`
- mix paper and live retry ownership

## Required policy

### Tier 1 — never suppress

These continue through the current degraded protective path:

- hard stop
- stop hit
- EOD force close
- sentinel force close
- theta stop
- time stop
- never-green stop
- runner trail
- trailing stop
- profit lock
- touched-profit stop
- small-win lock
- existing profit-protection windows

### Tier 2 — retryable stale decision

For actionable exits such as immediate take-profit, TP scale-out, or underlying-progress exit, stale quote handling must produce an explicit bounded retry rather than a silent `continue`.

Required in-memory/durable fields:

- `exit_retry_owner`
- `exit_retry_reason`
- `exit_retry_decision_code`
- `exit_retry_requested_at`
- `exit_retry_at`
- `exit_retry_count`
- `exit_retry_max_attempts`
- `exit_retry_last_quote_state`
- `exit_retry_last_quote_age_sec`
- `exit_retry_last_error`

## Invariants

1. An actionable stale decision has exactly one owner.
2. Retry is bounded by attempts and wall-clock age.
3. Existing `exit_in_flight` identity always blocks duplicate submission.
4. Fresh quote arrival wakes the engine and re-evaluates the original decision.
5. Broker reconciliation continues independently of retry state.
6. Emergency exits bypass retry state.
7. Retry state is scoped by `position_id`, `client_id`, and `execution_mode`.
8. Restart recovery reloads pending retry state or deterministically reconstructs it.
9. Exhausted retry emits a durable critical reason; it does not silently disappear.
10. Paper retries can never mutate live positions or live orders, and vice versa.

## Suggested defaults

- first retry: next normal quote cycle
- maximum retry age: 30 seconds for TP/scale-out decisions
- maximum attempts: 5
- immediate re-evaluation on fresh quote wake

These defaults must remain configurable and tested; do not hard-code production behavior without env-backed constants.

## Required tests

1. Stale `IMMEDIATE_TP` creates one retry owner and no broker POST.
2. Fresh quote on next cycle clears retry and submits once.
3. Repeated stale cycles do not create duplicate retry records.
4. Active exit order identity prevents duplicate submission after retry wake.
5. Hard stop ignores pending soft-exit retry and submits protective exit.
6. EOD force close ignores pending retry.
7. Retry survives restart or is reconstructed deterministically.
8. Retry exhaustion writes exact terminal diagnostic state.
9. Wrong client or execution mode cannot claim retry ownership.
10. Reconciler import of broker-open position restores retry-capable monitoring.

## Acceptance

No path may log `OPTION_QUOTE_STALE_DECISION_SUPPRESSED` and then return without either:

- submitting an exempt protective exit,
- scheduling a bounded retry with ownership, or
- writing an explicit terminal safety escalation.
