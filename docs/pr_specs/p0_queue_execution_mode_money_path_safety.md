# P0 Queue Execution-Mode and Money-Path Safety

## Status

DRAFT IMPLEMENTATION CONTRACT ONLY. Production logic is not implemented by this branch. Do not merge until code and tests are added.

## Problem

`ap/queue.py` validates runtime execution mode early in `_dispatch()`, but the OSM entry create path still derives the actual order `execution_mode` from `execution_mode_for_broker(broker)`. If the runtime mode and broker mode disagree, the queue can create orders under the wrong paper/live taxonomy.

This is especially dangerous around PAPER recovery immediate promotion. That path is intended to remain paper-only, but its paper/live protection currently depends on `master_control.mode`, while broker submit uses the broker object. A runtime/broker mismatch can turn a paper-only recovery path into a live-money risk.

The file also still permits missing `contract_selector` to continue toward placeholder sizing, keeps broker equity cache freshness on a shared `master_control._equity_cache_ts`, and updates at least one `ap_signals` row by `signal_id` alone without `client_email` scoping.

## Scope

Touch only queue entry dispatch safety and diagnostics.

Allowed files:

- `ap/queue.py`
- focused tests under `tests/`

Do not touch scanners, scoring, intelligence, exits, reconciler, proof taxonomy, or broker adapters except through test fakes.

## Required behavior

### 1. Runtime mode must match broker mode before OSM create

Before `order_state_machine.create_entry_order(...)`, compute:

```python
broker_exec_mode = execution_mode_for_broker(broker).upper()
```

Then fail closed if:

```python
broker_exec_mode != _execution_mode
```

Required queue result:

```json
{
  "stage": "execution_mode_validation",
  "reason": "metadata_invalid:runtime_broker_execution_mode_mismatch",
  "reason_code": "metadata_invalid:runtime_broker_execution_mode_mismatch",
  "runtime_execution_mode": "...",
  "broker_execution_mode": "..."
}
```

Required `last_error`:

```text
metadata_invalid:runtime_broker_execution_mode_mismatch
```

No OSM order may be created. No watcher may be armed. No broker submit may happen.

### 2. PAPER recovery immediate promotion must assert broker mode is PAPER

Inside the `PAPER_RECOVERY_IMMEDIATE_PROMOTE` block, after the normal `not live_mode` runtime check, also require:

```python
broker_exec_mode == "PAPER"
```

If not, reject/fail closed before promotion with reason:

```text
paper_recovery_immediate_broker_mode_mismatch
```

No immediate submit path may run.

### 3. Missing contract selector must fail closed unless verified contract is already present

If `contract_selector` is missing, queue may continue only if all are true:

- `plan.contract_symbol` exists and is not a `DEFERRED:` placeholder
- `plan.contracts > 0`
- `plan.limit_price > 0` or equivalent verified selected premium exists
- `plan.max_position_usd > 0`

LIVE must fail closed without these values. PAPER should also reject unless a test-mode explicit bypass is present.

Required reason:

```text
contract_selection:contract_selector_required
```

### 4. Equity cache must be per client

Replace shared `master_control._equity_cache_ts` freshness with a dict keyed by `client_id` and, if available, broker account id.

Example shape:

```python
cache = getattr(master_control, "_equity_cache", {})
entry = cache.get(cache_key)
```

Do not let one client refresh skip another client's equity refresh.

### 5. ap_signals updates must be client-scoped

Any direct update such as:

```python
_sb.table("ap_signals").update(...).eq("signal_id", signal_id).execute()
```

must include:

```python
.eq("client_email", canonical_client_email(client_id))
```

### 6. Score conversion must use `_safe_float()` in dispatch diagnostics

Replace `float(payload.get("score") or 0)` in rejection/audit/logging paths that can see old queue rows with `_safe_float(...)`.

## Acceptance tests

Add focused tests that prove:

1. LIVE runtime + PAPER broker rejects before OSM create.
2. PAPER runtime + LIVE broker rejects before OSM create.
3. PAPER recovery immediate + PAPER broker can promote when contract/qty/limit are valid.
4. PAPER recovery immediate + LIVE broker is blocked before immediate submit.
5. Missing contract selector rejects before OSM create unless verified contract metadata exists.
6. Equity freshness is per-client, not shared across client ids.
7. Same `signal_id` across two clients updates only the matching `client_email` row.
8. Malformed score such as `"A+"` does not crash rejection logging.

## Merge gate

Merge only when exact-head focused tests pass and diff shows no changes to scanner admission, scoring, exits, reconciler, proof taxonomy, or queue fanout.
